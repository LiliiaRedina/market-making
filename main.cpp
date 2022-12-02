#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <queue>
#include <deque>

std::vector<std::string> split(const std::string& s, char delim) {
    std::stringstream ss(s);
    std::string token;
    std::vector<std::string> tokens;
    while (getline(ss, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}

std::vector<std::vector<std::string>> readFile(const std::string& path) {
    std::ifstream file(path);
    if (!file.good()) {
        std::cerr << path << " not found" << std::endl;
        exit(1);
    }
    std::string line;
    std::vector<std::vector<std::string>> units;
    int k = 0;
    std::getline(file, line);
    while (k++ < 10000 && std::getline(file, line)) {
        std::vector<std::string> tokens = split(line, ',');
        units.push_back(tokens);
    }
    return units;
}

enum OrderType {ASK, BID};
enum ActWithOrder {EXECUTE, KILL, SKIP};
enum ActionType {CANCEL, PLACE};
enum MarketDataType {ORDERBOOK, TRADE};
enum FeedbackType {EXECUTE_ORDER, CANCEL_ORDER};

class Orderbook {
public:
    std::vector<std::pair<double, double>> ask_orderbook;
    std::vector<std::pair<double, double>> bid_orderbook;
};

class Trade {
public:
    OrderType type;
    double price;
    double volume;
};

class MarketData {
public:
    MarketDataType type;
    long long receive_ts;
    long long exchange_ts;
    Orderbook orderbook;
    Trade trade;

    MarketData() = default;

    MarketData(const std::vector<std::string>& line, MarketDataType type) {
        this->type = type;
        if (type == ORDERBOOK) {
            receive_ts = std::stoll(line[0]);
            exchange_ts = std::stoll(line[1]);
            for (size_t i = 2; i + 3 < line.size(); i += 4) {
                orderbook.ask_orderbook.emplace_back(std::stod(line[i]), std::stod(line[i + 1]));
                orderbook.bid_orderbook.emplace_back(std::stod(line[i + 2]), std::stod(line[i + 3]));
            }
        } else if (type == TRADE) {
            receive_ts = std::stoll(line[0]);
            exchange_ts = std::stoll(line[1]);
            trade.type = line[2] == "BID" ? BID : ASK;
            trade.price = std::stod(line[3]);
            trade.volume = std::stod(line[4]);
        }
    }
};

class Order {
public:
    double price;
    double volume;
    long long kill_time;
    OrderType type;
};

class Feedback {
public:
    FeedbackType type;
    size_t id_order;
    long long time;
};

class Action {
public:
    ActionType type;
    long long time;
    Order order;
    size_t id_order;

    Action(ActionType type, long long time, Order order, size_t id_order) : type(type), time(time),
                                                                            order(order), id_order(id_order) {}
    Action(ActionType type, long long time, size_t id_order) : type(type), time(time), id_order(id_order) {}
};

class Simulator {
public:
    long long ts;
    long long execute_latency;
    long long feedback_latency;
    std::deque<MarketData> market_orderbooks;
    std::deque<MarketData> market_trades;
    size_t index_market_orderbooks;
    size_t index_market_trades;
    std::queue<Action> actions;
    std::queue<Feedback> strategy_updates;
    std::unordered_map<size_t, Order> active_orders;
    size_t id_order_counter;

    Simulator(long long init_ts, long long execute_latency, long long feedback_latency,
              const std::string& path_orderbooks, const std::string& path_trades):
            ts(init_ts), execute_latency(execute_latency), feedback_latency(feedback_latency),
            index_market_orderbooks(0), index_market_trades(0), id_order_counter(0) {
        std::vector<std::vector<std::string>> data_orderbook = readFile(path_orderbooks);
        for (auto& line : data_orderbook) {
            market_orderbooks.emplace_back(line, ORDERBOOK);
        }

        std::vector<std::vector<std::string>> data_trades = readFile(path_trades);
        for (auto& line : data_trades) {
            market_trades.emplace_back(line, TRADE);
        }
    }

    void update_market() {
//        std::cout << "uuuuuuuuuuuuuuuuuuu " << index_market_orderbooks << " " << market_orderbooks.size() << " " << market_orderbooks[index_market_orderbooks].exchange_ts << " " << ts << '\n';
        MarketData market_data;
        while((index_market_orderbooks < market_orderbooks.size() && market_orderbooks[index_market_orderbooks].exchange_ts < ts)
              || (index_market_trades < market_trades.size() && market_trades[index_market_trades].exchange_ts < ts)) {
            if (index_market_trades >= market_trades.size() || (index_market_orderbooks < market_orderbooks.size() &&
                                                                market_orderbooks[index_market_orderbooks].exchange_ts < market_trades[index_market_trades].exchange_ts)) {
                market_data = market_orderbooks[index_market_orderbooks];
                ++index_market_orderbooks;
            } else {
                market_data = market_trades[index_market_trades];
                ++index_market_trades;
            }
            check_orders(market_data);
            execute_orders(market_data);
        }
    }

    void check_orders(MarketData market_data) {
        while (!actions.empty() && actions.front().time < market_data.exchange_ts) {
            prepare_orders(actions.front());
            actions.pop();
        }
    }

    void execute_orders(MarketData market_data) {
        std::vector<size_t> id_cancel;
        for (auto& [id, order] : active_orders) {
            if (market_data.type == ORDERBOOK) {
                if (order.type == BID) {
                    if (market_data.orderbook.ask_orderbook[0].first <= order.price) {
                        strategy_updates.push({EXECUTE_ORDER, id, market_data.exchange_ts + feedback_latency});
                        id_cancel.push_back(id);
                    }
                } else if (order.type == ASK) {
                    if (market_data.orderbook.bid_orderbook[0].first >= order.price) {
                        strategy_updates.push({EXECUTE_ORDER, id, market_data.exchange_ts + feedback_latency});
                        id_cancel.push_back(id);
                    }
                }
            } else if (market_data.type == TRADE) {
                if (order.type == BID && market_data.trade.type == ASK && market_data.trade.price <= order.price ||
                    order.type == ASK && market_data.trade.type == BID && market_data.trade.price >= order.price) {
                    strategy_updates.push({EXECUTE_ORDER, id, market_data.exchange_ts + feedback_latency});
                    id_cancel.push_back(id);
                }
            }
        }
        for (auto id : id_cancel) {
            active_orders.erase(id);
        }
    }

    void prepare_orders(Action action) {
        if (action.type == CANCEL) {
            if (active_orders.find(action.id_order) != nullptr) {
                active_orders.erase(action.id_order);
                strategy_updates.push({CANCEL_ORDER, action.id_order, action.time + feedback_latency});
            }
        } else if (action.type == PLACE) {
            active_orders.insert({action.id_order, action.order});
        }
    }

    MarketData get_new_market_data() {
        MarketData new_md;
        if (market_trades.empty() || (!market_orderbooks.empty() &&
                                      market_orderbooks.front().receive_ts < market_trades.front().receive_ts)) {
            new_md = market_orderbooks.front();
            ts = new_md.receive_ts;
            update_market();
            market_orderbooks.pop_front();
            if (index_market_orderbooks != 0) {
                --index_market_orderbooks;
            }
            return new_md;
        } else {
            new_md = market_trades.front();
            ts = new_md.receive_ts;
            update_market();
            market_trades.pop_front();
            if (index_market_trades != 0) {
                --index_market_trades;
            }
            return new_md;
        }
    }

    std::vector<Feedback> update_feedback() {
        std::vector<Feedback> feedback;
        while (!strategy_updates.empty() && strategy_updates.front().time < ts) {
            feedback.push_back(strategy_updates.front());
            strategy_updates.pop();
        }

        return feedback;
    }

    std::pair<MarketData, std::vector<Feedback>> tick() {
        if (market_orderbooks.empty() && market_trades.empty()) {
            return {};
        }

        MarketData new_market_data = get_new_market_data();
        std::vector<Feedback> feedback = update_feedback();

        return {new_market_data, feedback};
    }

    size_t place_order(const Order& order) {
        actions.push({PLACE, ts + execute_latency, order, id_order_counter});
//        std::cout << actions.size() << '\n';
        return id_order_counter++;
    }

    void cancel_order(size_t id_order) {
        actions.push({CANCEL, ts + execute_latency, id_order});
    }
};

int main() {
    const long long INIT_RECEIVED = 1655942402249000000;
    const long long FEEDBACK_LATENCY = 1000000000;
    const long long POST_LATENCY = 1000000000;
    Simulator simulator(INIT_RECEIVED, FEEDBACK_LATENCY, POST_LATENCY,
                        "D:/CLionProjects/untitled/md/md/btcusdt_Binance_LinearPerpetual/lobs.csv",
                        "D:/CLionProjects/untitled/md/md/btcusdt_Binance_LinearPerpetual/trades.csv");

    double capital_usdt = 0;
    double capital_btc = 0;
    std::vector<double> capital_usdt_history;
    std::vector<double> capital_btc_history;
    std::vector<double> capital_sum_history;

    auto res = simulator.tick();
    MarketData current= res.first;
    MarketData previous;
    while (current.type == TRADE) {
        res = simulator.tick();
        current = res.first;
    }
    res = simulator.tick();
    while (res.first.type == TRADE) {
        res = simulator.tick();
    }

    std::unordered_map<size_t, Order> orders;

    while(!simulator.market_orderbooks.empty() || !simulator.market_trades.empty()) {
//        std::cout << simulator.ts <<  ' ' << simulator.strategy_updates.size() << '\n';

        previous = current;
        current = res.first;

        Order order;
        double mid_volume = (current.orderbook.ask_orderbook[0].second + current.orderbook.bid_orderbook[0].second) / 2;
        double mid_price_current = (current.orderbook.ask_orderbook[0].first + current.orderbook.bid_orderbook[0].first) / 2;
        double mid_price_previous = (previous.orderbook.ask_orderbook[0].first + previous.orderbook.bid_orderbook[0].first) / 2;
        if (mid_price_current < mid_price_previous) {
            order.type = ASK;
        } else {
            order.type = BID;
        }
        order.price = mid_price_current;
        order.volume = mid_volume;
        order.kill_time = 3000000000000;

        size_t id_order = simulator.place_order(order);
        orders.insert({id_order, order});

        for (auto& feedback : res.second) {
            if (feedback.type == EXECUTE_ORDER) {
                if (orders[feedback.id_order].type == ASK) {
                    capital_usdt += orders[feedback.id_order].price * orders[feedback.id_order].volume;
                    capital_btc -= orders[feedback.id_order].volume;
                } else if (orders[feedback.id_order].type == BID) {
                    capital_usdt -= orders[feedback.id_order].price * orders[feedback.id_order].volume;
                    capital_btc += orders[feedback.id_order].volume;
                }
            }
        }

        capital_usdt_history.push_back(capital_usdt);
        capital_btc_history.push_back(capital_btc);
        capital_sum_history.push_back(capital_usdt + capital_btc * mid_price_current);

        res = simulator.tick();
        while (res.first.type == TRADE) {
            res = simulator.tick();
        }
    }

    std::ofstream file("D:/CLionProjects/untitled/results.txt");
    for (auto capital : capital_usdt_history) {
        file << capital << " ";
    }
    file << '\n';
    for (auto capital : capital_btc_history) {
        file << capital << " ";
    }
    file << '\n';
    for (auto capital : capital_sum_history) {
        file << capital << " ";
    }
    file << '\n';

    return 0;
}
