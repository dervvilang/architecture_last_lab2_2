#include <unistd.h>
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <chrono>
#include <csignal>
#include <thread>
#include <amqp.h>
#include <amqp_tcp_socket.h>

static bool stop_consumer = false;
void handle_signal(int sig) {
    if (sig == SIGINT) {
        std::cout << "\n[Consumer] Завершение работы по сигналу Ctrl+C" << std::endl;
        stop_consumer = true;
    }
}

static void die_on_amqp_error(amqp_rpc_reply_t x, const char* context) {
    if (x.reply_type == AMQP_RESPONSE_NORMAL)
        return;
    std::cerr << "[AMQP] Error in " << context << std::endl;
    exit(1);
}

// Умножение матриц и подсчёт суммы
std::pair<long long, long long> multiply_and_sum(const std::vector<std::vector<int>>& A,
                                                 const std::vector<std::vector<int>>& B) {
    int N = A.size();
    std::vector<std::vector<long long>> C(N, std::vector<long long>(N, 0));
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
            for (int k = 0; k < N; k++) {
                C[i][j] += static_cast<long long>(A[i][k]) * B[k][j];
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    long long sum = 0;
    for (int i = 0; i < N; i++)
        for (int j = 0; j < N; j++)
            sum += C[i][j];

    return {sum, duration_ms};
}

// Разбор строки: сначала N, потом 2*N*N чисел, возвращает матрицы A и B
bool parse_task(const std::string &data, std::vector<std::vector<int>>& A, std::vector<std::vector<int>>& B) {
    std::istringstream iss(data);
    int N;
    if (!(iss >> N)) {
        return false;
    }
    A.assign(N, std::vector<int>(N, 0));
    B.assign(N, std::vector<int>(N, 0));

    for (int i = 0; i < N; i++)
        for (int j = 0; j < N; j++)
            if (!(iss >> A[i][j]))
                return false;

    for (int i = 0; i < N; i++)
        for (int j = 0; j < N; j++)
            if (!(iss >> B[i][j]))
                return false;

    return true;
}

int main(int argc, char* argv[]) {
    signal(SIGINT, handle_signal);

    std::string rabbit_host = "rabbitmq";
    int rabbit_port = 5672;

    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        std::cerr << "[Consumer] Ошибка создания TCP-сокета RabbitMQ\n";
        return 1;
    }

    // Цикл повторных попыток подключения к RabbitMQ
    int status;
    while ((status = amqp_socket_open(socket, rabbit_host.c_str(), rabbit_port)) != 0) {
        std::cerr << "[Consumer] Не удалось подключиться к " << rabbit_host << ":" << rabbit_port
                  << ". Повтор через 3 сек...\n";
        sleep(3);
    }

    die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0,
                                 AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
                      "Logging in");
    amqp_channel_open(conn, 1);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

    // Объявляем очереди
    {
        amqp_queue_declare_ok_t* r = amqp_queue_declare(
            conn, 1, amqp_cstring_bytes("tasks_queue"),
            0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring tasks_queue");
    }
    {
        amqp_queue_declare_ok_t* r = amqp_queue_declare(
            conn, 1, amqp_cstring_bytes("results_queue"),
            0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring results_queue");
    }

    // Подписываемся на очередь tasks_queue
    amqp_basic_consume(conn,
                       1,
                       amqp_cstring_bytes("tasks_queue"),
                       amqp_empty_bytes, // consumer_tag
                       0, // no_local
                       0, // no_ack = false (подтверждаем вручную)
                       0, // exclusive
                       amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming from tasks_queue");

    std::cout << "[Consumer] Ожидание задач в tasks_queue..." << std::endl;

    while (!stop_consumer) {
        amqp_rpc_reply_t res;
        amqp_envelope_t envelope;

        amqp_maybe_release_buffers(conn);
        res = amqp_consume_message(conn, &envelope, NULL, 0);

        if (AMQP_RESPONSE_NORMAL != res.reply_type) {
            if (stop_consumer)
                break;
            continue;
        }

        std::string task_data(reinterpret_cast<char*>(envelope.message.body.bytes),
                              envelope.message.body.len);

        std::vector<std::vector<int>> A, B;
        if (!parse_task(task_data, A, B)) {
            std::cerr << "[Consumer] Ошибка парсинга задания!\n";
            amqp_basic_ack(conn, 1, envelope.delivery_tag, false);
            amqp_destroy_envelope(&envelope);
            continue;
        }

        auto [sum, duration_ms] = multiply_and_sum(A, B);
        std::cout << "[Consumer] Выполнено умножение, сумма=" << sum
                  << ", время=" << duration_ms << " мс" << std::endl;

        // Отправляем результат в results_queue
        {
            std::string result = "SUM=" + std::to_string(sum) +
                                 " TIME=" + std::to_string(duration_ms) + "ms";
            amqp_bytes_t message_bytes;
            message_bytes.len = result.size();
            message_bytes.bytes = (void*)result.data();

            amqp_basic_publish(conn,
                               1,
                               amqp_cstring_bytes(""),
                               amqp_cstring_bytes("results_queue"),
                               0, 0, NULL, message_bytes);
        }

        // Подтверждаем получение сообщения
        amqp_basic_ack(conn, 1, envelope.delivery_tag, false);
        amqp_destroy_envelope(&envelope);
    }

    // Закрываем соединение
    die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
    amqp_destroy_connection(conn);

    return 0;
}
