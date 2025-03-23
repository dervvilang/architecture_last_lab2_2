#include <unistd.h>
#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <csignal>
#include <amqp.h>
#include <amqp_tcp_socket.h>

static void die_on_amqp_error(amqp_rpc_reply_t x, const char* context) {
    if (x.reply_type == AMQP_RESPONSE_NORMAL)
        return;
    std::cerr << "[AMQP] Error in " << context << std::endl;
    exit(1);
}

bool stop_producer = false;
void handle_signal(int sig) {
    if (sig == SIGINT) {
        std::cout << "\n[Producer] Завершается по сигналу Ctrl+C" << std::endl;
        stop_producer = true;
    }
}

std::string generate_task(int matrix_size) {
    srand(static_cast<unsigned>(time(nullptr)) + rand());
    std::string result = std::to_string(matrix_size) + " ";
    int total_elements = matrix_size * matrix_size;
    for (int i = 0; i < total_elements; i++) {
        int val = rand() % 10;
        result += std::to_string(val) + " ";
    }
    for (int i = 0; i < total_elements; i++) {
        int val = rand() % 10;
        result += std::to_string(val) + " ";
    }
    return result;
}

void send_message(amqp_connection_state_t conn, const std::string &queue_name, const std::string &message_body) {
    amqp_bytes_t message_bytes;
    message_bytes.len = message_body.size();
    message_bytes.bytes = (void*)message_body.data();

    amqp_basic_publish(conn,
                       1,
                       amqp_cstring_bytes(""),
                       amqp_cstring_bytes(queue_name.c_str()),
                       0, 0,
                       NULL,
                       message_bytes);
}

int main(int argc, char* argv[]) {
    std::string rabbit_host = "rabbitmq";
    int rabbit_port = 5672;
    int task_count = 10;
    bool run_infinite = false;
    int matrix_size = 10;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-n" && i + 1 < argc) {
            task_count = std::stoi(argv[++i]);
        } else if (arg == "-m" && i + 1 < argc) {
            matrix_size = std::stoi(argv[++i]);
        } else if (arg == "-i") {
            run_infinite = true;
        }
    }

    signal(SIGINT, handle_signal);

    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        std::cerr << "[Producer] Ошибка создания TCP-сокета!" << std::endl;
        return 1;
    }

    // Цикл повторных попыток подключения к RabbitMQ
    int status;
    while ((status = amqp_socket_open(socket, rabbit_host.c_str(), rabbit_port)) != 0) {
        std::cerr << "[Producer] Не удалось подключиться к " << rabbit_host << ":" << rabbit_port
                  << ". Повтор через 3 сек...\n";
        sleep(3);
    }

    die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                                 "guest", "guest"),
                      "Logging in");
    amqp_channel_open(conn, 1);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

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

    std::cout << "[Producer] Начинаем отправку..." << std::endl;
    int tasks_sent = 0;
    auto start_all = std::chrono::high_resolution_clock::now();

    while (!stop_producer && (run_infinite || tasks_sent < task_count)) {
        std::string task_body = generate_task(matrix_size);
        send_message(conn, "tasks_queue", task_body);
        tasks_sent++;
        std::cout << "[Producer] Отправлено задание #" << tasks_sent
                  << " (матрица " << matrix_size << "x" << matrix_size << ")" << std::endl;
        usleep(200000);
    }

    auto end_all = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_all - start_all).count();
    std::cout << "[Producer] Всего отправлено " << tasks_sent
              << " заданий, общее время: " << ms << " мс" << std::endl;

    die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
    amqp_destroy_connection(conn);

    return 0;
}
