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

// Глобальная переменная для хранения суммарного времени обработки (мс).
static long long total_processing_time_ms = 0;

// Флаг для остановки по сигналу.
static bool stop_consumer = false;

// Сколько раз подряд мы получили таймаут (нет сообщений).
static int consecutive_timeouts = 0;
// Сколько "пустых" таймаутов подряд считаем признаком, что очередь опустела.
static const int MAX_CONSECUTIVE_TIMEOUTS = 10;

// Вызываем при ошибках из amqp_* функций.
static void die_on_amqp_error(amqp_rpc_reply_t x, const char* context) {
    if (x.reply_type == AMQP_RESPONSE_NORMAL) {
        return;
    }
    std::cerr << "[AMQP] Error in " << context << std::endl;
    exit(1);
}

// Обработчик сигналов SIGINT / SIGTERM.
void handle_stop_signal(int sig) {
    if (sig == SIGINT) {
        std::cout << "\n[Consumer] Завершается по сигналу Ctrl+C" << std::endl;
    } else if (sig == SIGTERM) {
        std::cout << "\n[Consumer] Завершается по сигналу SIGTERM (docker stop)" << std::endl;
    }
    stop_consumer = true;
}

// Умножение двух матриц A, B размером size x size.
std::vector<long long> multiply_matrices(const std::vector<long long>& A,
                                         const std::vector<long long>& B,
                                         int size) 
{
    std::vector<long long> C(size * size, 0);
    for (int i = 0; i < size; ++i) {
        for (int j = 0; j < size; ++j) {
            long long sum = 0;
            for (int k = 0; k < size; ++k) {
                sum += A[i * size + k] * B[k * size + j];
            }
            C[i * size + j] = sum;
        }
    }
    return C;
}

// Разбираем строку: сначала N, затем N*N чисел для A, затем N*N чисел для B.
bool parse_task(const std::string &message, 
                int &matrix_size, 
                std::vector<long long> &A, 
                std::vector<long long> &B) 
{
    std::vector<long long> tokens;
    tokens.reserve(message.size()/2); // грубая оценка

    {
        size_t start = 0;
        while (true) {
            size_t space_pos = message.find(' ', start);
            if (space_pos == std::string::npos) {
                // Последний кусок
                std::string part = message.substr(start);
                if (!part.empty()) {
                    try {
                        tokens.push_back(std::stoll(part));
                    } catch (...) {
                        return false;
                    }
                }
                break;
            } else {
                std::string part = message.substr(start, space_pos - start);
                if (!part.empty()) {
                    try {
                        tokens.push_back(std::stoll(part));
                    } catch (...) {
                        return false;
                    }
                }
            }
            start = space_pos + 1;
        }
    }

    if (tokens.empty()) {
        return false;
    }

    matrix_size = static_cast<int>(tokens[0]);
    if (matrix_size <= 0) {
        return false;
    }
    // Нужно 1 (для N) + 2 * (N*N) чисел.
    const int needed = 1 + 2 * (matrix_size * matrix_size);
    if ((int)tokens.size() < needed) {
        return false;
    }

    A.resize(matrix_size * matrix_size);
    B.resize(matrix_size * matrix_size);

    int offset = 1;
    for (int i = 0; i < matrix_size * matrix_size; i++) {
        A[i] = tokens[offset + i];
    }
    offset = 1 + matrix_size * matrix_size;
    for (int i = 0; i < matrix_size * matrix_size; i++) {
        B[i] = tokens[offset + i];
    }

    return true;
}

int main(int argc, char* argv[]) {
    // Ловим SIGINT/SIGTERM, чтобы корректно завершаться.
    signal(SIGINT, handle_stop_signal);
    signal(SIGTERM, handle_stop_signal);

    std::string rabbit_host = "rabbitmq";
    int rabbit_port = 5672;

    // Создаём соединение.
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        std::cerr << "[Consumer] Ошибка создания TCP-сокета!" << std::endl;
        return 1;
    }

    // Цикл повторных попыток подключения.
    while (true) {
        int status = amqp_socket_open(socket, rabbit_host.c_str(), rabbit_port);
        if (status == 0) {
            break; // Успех
        }
        std::cerr << "[Consumer] Не удалось подключиться к " 
                  << rabbit_host << ":" << rabbit_port
                  << ". Повтор через 3 сек...\n";
        sleep(3);
    }

    // Логинимся, открываем канал.
    die_on_amqp_error(
        amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
        "Logging in"
    );
    amqp_channel_open(conn, 1);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

    // Декларируем очередь (если не создана).
    {
        amqp_queue_declare_ok_t* r = amqp_queue_declare(
            conn, 1, amqp_cstring_bytes("tasks_queue"),
            0, 0, 0, 0, amqp_empty_table
        );
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring tasks_queue");
    }

    // Подписываемся на очередь.
    amqp_basic_consume(
        conn,
        1,
        amqp_cstring_bytes("tasks_queue"),
        amqp_empty_bytes,
        0,   // no-local
        0,   // no-ack = false, значит будем ack'ать вручную
        0,   // exclusive
        amqp_empty_table
    );
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");

    std::cout << "[Consumer] Готов к приёму сообщений..." << std::endl;

    // Основной цикл чтения.
    while (!stop_consumer) {
        amqp_rpc_reply_t res;
        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);

        // Таймаут чтения 1 сек (для проверки stop_consumer и пустой очереди).
        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        res = amqp_consume_message(conn, &envelope, &timeout, 0);
        if (stop_consumer) {
            // Сигнал – выходим.
            amqp_destroy_envelope(&envelope);
            break;
        }

        if (res.reply_type == AMQP_RESPONSE_NORMAL) {
            // Сбросим счётчик «пустых» раз.
            consecutive_timeouts = 0;

            // Фиксируем время начала обработки.
            auto start_time = std::chrono::high_resolution_clock::now();

            // Тело сообщения.
            std::string body(
                (char*)envelope.message.body.bytes, 
                envelope.message.body.len
            );

            int matrix_size = 0;
            std::vector<long long> A, B;
            bool ok = parse_task(body, matrix_size, A, B);
            long long result_sum = 0;
            if (ok) {
                // Умножаем.
                auto C = multiply_matrices(A, B, matrix_size);
                // Считаем сумму для демонстрации.
                for (auto &v : C) {
                    result_sum += v;
                }
            } else {
                std::cerr << "[Consumer] Ошибка парсинга: " << body << std::endl;
            }

            // Конец обработки.
            auto end_time = std::chrono::high_resolution_clock::now();
            long long time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    end_time - start_time
                                ).count();

            // Прибавляем время.
            total_processing_time_ms += time_ms;

            // Логируем.
            std::cout << "[Consumer] Выполнено умножение, сумма=" 
                      << result_sum << ", время=" << time_ms << " мс" 
                      << std::endl;

            // Подтверждаем (ack).
            amqp_basic_ack(conn, 1, envelope.delivery_tag, false);

            amqp_destroy_envelope(&envelope);

        } else if (res.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION 
                && res.library_error == AMQP_STATUS_TIMEOUT) {
            // Нет новых сообщений в течение 1 секунды.
            consecutive_timeouts++;
            // Если подряд несколько таймаутов – считаем, что очередь пуста.
            if (consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS) {
                std::cout << "[Consumer] Нет сообщений " << consecutive_timeouts 
                          << " секунд подряд. Завершаем..." << std::endl;
                break;
            }
        } else {
            // Любая другая ошибка (закрытие канала и т.п.).
            std::cerr << "[Consumer] Ошибка чтения сообщения, завершаем..." 
                      << std::endl;
            amqp_destroy_envelope(&envelope);
            break;
        }
    }

    // При любом выходе из цикла – сигнал или «пустая» очередь.
    std::cout << "[Consumer] Суммарное время обработки всех заданий: " 
              << total_processing_time_ms << " мс" 
              << std::endl;

    // Закрываем канал и соединение.
    die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
    amqp_destroy_connection(conn);

    return 0;
}
