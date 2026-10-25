/*
 * Smart Bin - ESP32 (ESP-IDF) + MQTT com LWT - 3 LEDs
 * DistÃ¢ncia lida do HC-SR04; valores digitados no idf.py monitor sobrepÃµem o sensor.
 *
 * LEDs: Verde = NORMAL (<50%) | Amarelo = ATENCAO (50-79%) | Vermelho = CHEIA (>=80%)
 *
 * COMANDOS (digite no idf.py monitor e aperte Enter):
 *   sensor  -> volta a usar o HC-SR04 (padrÃ£o no boot)
 *   cal     -> mede e imprime a distÃ¢ncia (para calibrar), sem publicar
 *   20      -> fixa a distÃ¢ncia em 20 cm
 *   75%     -> fixa o nÃ­vel direto em 75%
 *   auto    -> ciclo automÃ¡tico com valores prÃ©-definidos
 *   manual  -> para o ciclo e mantÃ©m o Ãºltimo valor
 *   info    -> diagnÃ³stico de Wi-Fi, MQTT e estado
 *   ajuda   -> lista os comandos
 * Os mesmos comandos podem ser publicados no tÃ³pico <prefixo>/simular.
 *
 * TÃ³picos publicados (prefixo padrÃ£o smartbin/01):
 *   /nivel    -> 0..100
 *   /status   -> NORMAL | ATENCAO | CHEIA | ERRO_SENSOR (retido)
 *   /alerta   -> LIXEIRA_CHEIA (sÃ³ ao entrar no estado crÃ­tico)
 *   /conexao  -> ONLINE | OFFLINE (OFFLINE via LWT, retido)
 *
 * Wi-Fi, broker e prefixo: editar as variaveis no topo deste arquivo
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <ctype.h>
#include <unistd.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

#include "esp_system.h"
#include "esp_log.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_wifi.h"
#include "esp_mac.h"
#include "esp_timer.h"
#include "esp_rom_sys.h"
#include "nvs_flash.h"
#include "driver/gpio.h"
#include "mqtt_client.h"

static const char *TAG = "SMARTBIN";

// ================== CONFIGURAÃ‡ÃƒO ==================
static const char *WIFI_SSID     = "peterHenry";       // rede 2.4 GHz
static const char *WIFI_PASSWORD = "SUA_SENHA";
static const char *BROKER_URI    = "mqtt://broker.hivemq.com";
static const char *TOPIC_BASE    = "smartbin/01";

#define PIN_LED_VERDE     GPIO_NUM_25
#define PIN_LED_AMARELO   GPIO_NUM_26
#define PIN_LED_VERMELHO  GPIO_NUM_27
#define PIN_TRIG          GPIO_NUM_5    // HC-SR04
#define PIN_ECHO          GPIO_NUM_18

// Fonte no boot: 0 = manual (valores digitados) | 1 = HC-SR04 real
#define USAR_SENSOR_REAL  1

// Filtro do HC-SR04
#define SENSOR_AMOSTRAS       5
#define SENSOR_MIN_VALIDAS    3
#define SENSOR_INTERVALO_MS   60      // datasheet: >= 60 ms entre disparos
#define SENSOR_MIN_CM         2.0f
#define SENSOR_MAX_CM         400.0f
#define FALHAS_PARA_ERRO      3       // ciclos invÃ¡lidos seguidos -> ERRO_SENSOR

// CalibraÃ§Ã£o (medir na caixa de papelÃ£o)
#define DIST_VAZIA_CM     35.0f   // sensor -> fundo com a caixa vazia
#define DIST_CHEIA_CM     5.0f    // sensor -> resÃ­duo quando considerada cheia

#define LIMITE_ATENCAO    50
#define LIMITE_CHEIA      80

#define INTERVALO_MS      3000
#define MQTT_KEEPALIVE_S  10      // broker detecta queda em ~15 s

// SequÃªncia do modo "auto" (cm): 0% -> 17% -> 50% -> 77% -> 90% -> 100%
static const float DIST_SIMULADAS[] = {35.0f, 30.0f, 20.0f, 12.0f, 8.0f, 5.0f};
#define N_SIMULADAS (sizeof(DIST_SIMULADAS) / sizeof(DIST_SIMULADAS[0]))
// ==================================================

typedef enum { NORMAL, ATENCAO, CHEIA } estado_t;
typedef enum { FONTE_SENSOR, FONTE_MANUAL, FONTE_AUTO } fonte_t;

#define CMD_MAX 32
typedef struct { char texto[CMD_MAX]; } comando_t;

static QueueHandle_t fila_cmd;
static esp_mqtt_client_handle_t mqtt = NULL;
static volatile bool wifi_ok = false;
static volatile bool mqtt_ok = false;

static char T_NIVEL[64], T_STATUS[64], T_ALERTA[64], T_CONEXAO[64], T_SIMULAR[64];
static char client_id[32];

// Estado da aplicaÃ§Ã£o (acessado sÃ³ pela tarefa_app)
static fonte_t  fonte           = USAR_SENSOR_REAL ? FONTE_SENSOR : FONTE_MANUAL;
static size_t   indice_simulado = 0;
static int      falhas_sensor   = 0;       // ciclos invÃ¡lidos seguidos
static float    distancia_atual = DIST_VAZIA_CM;
static int      ultimo_pct      = -1;
static estado_t estado_anterior = NORMAL;
static bool     primeira_leitura = true;

// ---------- Utilidades ----------
static void enviar_comando(const char *txt)
{
    comando_t c;
    strncpy(c.texto, txt, CMD_MAX - 1);
    c.texto[CMD_MAX - 1] = '\0';
    if (xQueueSend(fila_cmd, &c, 0) != pdTRUE) {
        ESP_LOGW(TAG, "Fila de comandos cheia, comando descartado");
    }
}

static int calcular_percentual(float dist)
{
    float pct = (DIST_VAZIA_CM - dist) / (DIST_VAZIA_CM - DIST_CHEIA_CM) * 100.0f;
    if (pct < 0)   pct = 0;
    if (pct > 100) pct = 100;
    return (int)(pct + 0.5f);
}

static float percentual_para_distancia(float pct)
{
    return DIST_VAZIA_CM - (pct / 100.0f) * (DIST_VAZIA_CM - DIST_CHEIA_CM);
}

static estado_t classificar(int pct)
{
    if (pct >= LIMITE_CHEIA)   return CHEIA;
    if (pct >= LIMITE_ATENCAO) return ATENCAO;
    return NORMAL;
}

static const char *estado_texto(estado_t e)
{
    switch (e) {
    case CHEIA:   return "CHEIA";
    case ATENCAO: return "ATENCAO";
    default:      return "NORMAL";
    }
}

static const char *fonte_texto(fonte_t f)
{
    switch (f) {
    case FONTE_SENSOR: return "sensor";
    case FONTE_AUTO:   return "auto";
    default:           return "manual";
    }
}

static const char *motivo_wifi(int reason)
{
    switch (reason) {
    case WIFI_REASON_NO_AP_FOUND:            return "rede nao encontrada (nome errado ou rede 5 GHz)";
    case WIFI_REASON_AUTH_FAIL:              return "autenticacao falhou (senha?)";
    case WIFI_REASON_4WAY_HANDSHAKE_TIMEOUT: return "handshake expirou (senha errada?)";
    case WIFI_REASON_HANDSHAKE_TIMEOUT:      return "handshake expirou (senha errada?)";
    case WIFI_REASON_ASSOC_LEAVE:            return "desconexao solicitada";
    case WIFI_REASON_BEACON_TIMEOUT:         return "sinal perdido";
    default:                                 return "ver codigo na documentacao do esp_wifi";
    }
}

// ---------- Sensor ----------
// Uma mediÃ§Ã£o do HC-SR04 (-1 = timeout)
static float medir_uma_vez_cm(void)
{
    gpio_set_level(PIN_TRIG, 0);
    esp_rom_delay_us(2);
    gpio_set_level(PIN_TRIG, 1);
    esp_rom_delay_us(10);
    gpio_set_level(PIN_TRIG, 0);

    int64_t t0 = esp_timer_get_time();
    while (gpio_get_level(PIN_ECHO) == 0) {
        if (esp_timer_get_time() - t0 > 30000) return -1;   // eco nÃ£o comeÃ§ou
    }
    int64_t inicio = esp_timer_get_time();
    while (gpio_get_level(PIN_ECHO) == 1) {
        if (esp_timer_get_time() - inicio > 30000) return -1; // eco nÃ£o terminou
    }
    int64_t duracao = esp_timer_get_time() - inicio;
    return duracao * 0.0343f / 2.0f;
}

// Mediana de vÃ¡rias mediÃ§Ãµes vÃ¡lidas (-1 = poucas leituras vÃ¡lidas)
static float ler_sensor_cm(void)
{
    float validas[SENSOR_AMOSTRAS];
    int n = 0;
    for (int i = 0; i < SENSOR_AMOSTRAS; i++) {
        if (i > 0) vTaskDelay(pdMS_TO_TICKS(SENSOR_INTERVALO_MS));
        float d = medir_uma_vez_cm();
        if (d >= SENSOR_MIN_CM && d <= SENSOR_MAX_CM) validas[n++] = d;
    }
    if (n < SENSOR_MIN_VALIDAS) return -1;

    // ordenaÃ§Ã£o por inserÃ§Ã£o (no mÃ¡ximo 5 valores)
    for (int i = 1; i < n; i++) {
        float v = validas[i];
        int j = i - 1;
        while (j >= 0 && validas[j] > v) { validas[j + 1] = validas[j]; j--; }
        validas[j + 1] = v;
    }
    return validas[n / 2];
}

static float ler_distancia_cm(void)
{
    switch (fonte) {
    case FONTE_SENSOR: {
        float d = ler_sensor_cm();
        if (d >= 0) distancia_atual = d;
        return d;
    }
    case FONTE_AUTO:
        distancia_atual = DIST_SIMULADAS[indice_simulado];
        indice_simulado = (indice_simulado + 1) % N_SIMULADAS;
        return distancia_atual;
    default:
        return distancia_atual;
    }
}

// ---------- AtuaÃ§Ã£o local ----------
static void atualizar_leds(estado_t e)
{
    gpio_set_level(PIN_LED_VERDE,    e == NORMAL  ? 1 : 0);
    gpio_set_level(PIN_LED_AMARELO,  e == ATENCAO ? 1 : 0);
    gpio_set_level(PIN_LED_VERMELHO, e == CHEIA   ? 1 : 0);
}

static void configurar_gpios(void)
{
    const gpio_num_t leds[] = {PIN_LED_VERDE, PIN_LED_AMARELO, PIN_LED_VERMELHO};
    for (int i = 0; i < 3; i++) {
        gpio_reset_pin(leds[i]);
        gpio_set_direction(leds[i], GPIO_MODE_OUTPUT);
        gpio_set_level(leds[i], 0);
    }
    gpio_reset_pin(PIN_TRIG);
    gpio_set_direction(PIN_TRIG, GPIO_MODE_OUTPUT);
    gpio_reset_pin(PIN_ECHO);
    gpio_set_direction(PIN_ECHO, GPIO_MODE_INPUT);
    gpio_set_pull_mode(PIN_ECHO, GPIO_PULLDOWN_ONLY);   // fio solto lÃª 0 (timeout), nÃ£o ruÃ­do

    // Teste visual no boot: verde -> amarelo -> vermelho
    for (int i = 0; i < 3; i++) {
        gpio_set_level(leds[i], 1);
        vTaskDelay(pdMS_TO_TICKS(300));
        gpio_set_level(leds[i], 0);
    }
}

// ---------- PublicaÃ§Ã£o ----------
static void publicar(const char *topico, const char *msg, int qos, int retain)
{
    if (!mqtt_ok) {
        ESP_LOGW(TAG, "[MQTT] Offline, nao publicou %s", topico);
        return;
    }
    int id = esp_mqtt_client_publish(mqtt, topico, msg, 0, qos, retain);
    if (id < 0) ESP_LOGE(TAG, "[MQTT] Falha ao publicar em %s (ret=%d)", topico, id);
}

// ---------- Ciclo: Sense -> Process -> Actuate -> Publish ----------
static void executar_ciclo(void)
{
    float dist = ler_distancia_cm();
    if (dist < 0) {
        ESP_LOGW(TAG, "[SENSOR] Leitura invalida, ignorando ciclo");
        if (++falhas_sensor == FALHAS_PARA_ERRO) {
            ESP_LOGE(TAG, "[SENSOR] %d leituras invalidas seguidas: ERRO_SENSOR", FALHAS_PARA_ERRO);
            gpio_set_level(PIN_LED_VERDE, 0);
            gpio_set_level(PIN_LED_AMARELO, 0);
            gpio_set_level(PIN_LED_VERMELHO, 0);
            publicar(T_STATUS, "ERRO_SENSOR", 0, 1);
        }
        return;
    }
    falhas_sensor = 0;

    int pct = calcular_percentual(dist);
    estado_t estado = classificar(pct);

    atualizar_leds(estado);

    char buf[8];
    snprintf(buf, sizeof(buf), "%d", pct);
    publicar(T_NIVEL, buf, 0, 0);
    publicar(T_STATUS, estado_texto(estado), 0, 1);

    if (estado == CHEIA && (estado_anterior != CHEIA || primeira_leitura)) {
        publicar(T_ALERTA, "LIXEIRA_CHEIA", 1, 0);
        ESP_LOGW(TAG, "[ALERTA] LIXEIRA_CHEIA");
    }

    ESP_LOGI(TAG, "dist=%.1f cm | nivel=%d%% | status=%s | fonte=%s | mqtt=%s",
             dist, pct, estado_texto(estado), fonte_texto(fonte),
             mqtt_ok ? "ok" : "OFFLINE");

    ultimo_pct = pct;
    estado_anterior = estado;
    primeira_leitura = false;
}

// ---------- Comandos ----------
static void mostrar_ajuda(void)
{
    printf("\n===== COMANDOS =====\n"
           "  sensor -> usa o HC-SR04 (fonte atual: %s)\n"
           "  cal    -> mede e mostra a distancia (calibracao)\n"
           "  20     -> distancia em cm (35 = vazia, 5 = cheia)\n"
           "  75%%    -> nivel direto em %%\n"
           "  auto   -> ciclo automatico\n"
           "  manual -> para o ciclo\n"
           "  info   -> diagnostico\n"
           "  ajuda  -> esta lista\n"
           "====================\n", fonte_texto(fonte));
}

static void mostrar_info(void)
{
    printf("\n===== INFO =====\n");
    wifi_ap_record_t ap;
    if (wifi_ok && esp_wifi_sta_get_ap_info(&ap) == ESP_OK) {
        printf("Wi-Fi: conectado a '%s' | sinal %d dBm\n", (char *)ap.ssid, ap.rssi);
    } else {
        printf("Wi-Fi: DESCONECTADO\n");
    }
    printf("MQTT: %s | broker %s\n", mqtt_ok ? "conectado" : "DESCONECTADO", BROKER_URI);
    printf("Client ID: %s\n", client_id);
    printf("Fonte: %s\n", fonte_texto(fonte));
    if (falhas_sensor > 0) printf("Leituras invalidas seguidas: %d\n", falhas_sensor);
    printf("Distancia: %.1f cm | Nivel: %d%% | Status: %s\n",
           distancia_atual, ultimo_pct, estado_texto(estado_anterior));
    printf("Memoria livre: %lu bytes\n", (unsigned long)esp_get_free_heap_size());
    printf("================\n");
}

// Retorna true se um ciclo deve rodar imediatamente
static bool aplicar_comando(char *cmd)
{
    // remove espaÃ§os nas pontas
    while (isspace((unsigned char)*cmd)) cmd++;
    size_t n = strlen(cmd);
    while (n > 0 && isspace((unsigned char)cmd[n - 1])) cmd[--n] = '\0';
    if (n == 0) return false;

    if (strcmp(cmd, "!ciclo") == 0) return true;   // interno (apÃ³s conectar MQTT)
    if (strcasecmp(cmd, "sensor") == 0) { fonte = FONTE_SENSOR; printf("[CMD] Fonte: sensor\n"); return true; }
    if (strcasecmp(cmd, "auto") == 0)   { fonte = FONTE_AUTO;   printf("[CMD] Fonte: auto\n"); return true; }
    if (strcasecmp(cmd, "manual") == 0) { fonte = FONTE_MANUAL; printf("[CMD] Fonte: manual (%.1f cm)\n", distancia_atual); return false; }
    if (strcasecmp(cmd, "cal") == 0) {
        float d = ler_sensor_cm();
        if (d < 0) printf("[CAL] Leitura invalida (confira fios e alimentacao do sensor)\n");
        else       printf("[CAL] Distancia: %.1f cm\n", d);
        return false;
    }
    if (strcasecmp(cmd, "info") == 0)   { mostrar_info();  return false; }
    if (strcasecmp(cmd, "ajuda") == 0)  { mostrar_ajuda(); return false; }

    bool eh_percentual = false;
    if (cmd[n - 1] == '%') { eh_percentual = true; cmd[--n] = '\0'; }

    bool valido = n > 0;
    for (size_t i = 0; i < n; i++) {
        if (cmd[i] == ',') cmd[i] = '.';
        if (!isdigit((unsigned char)cmd[i]) && cmd[i] != '.') { valido = false; break; }
    }
    if (!valido) {
        printf("[CMD] '%s' nao reconhecido. Digite 'ajuda'.\n", cmd);
        return false;
    }

    float v = strtof(cmd, NULL);
    fonte = FONTE_MANUAL;
    if (eh_percentual) {
        if (v > 100) v = 100;
        distancia_atual = percentual_para_distancia(v);
        printf("[CMD] Nivel fixado em %.0f%% (= %.1f cm)\n", v, distancia_atual);
    } else {
        distancia_atual = v;
        printf("[CMD] Distancia fixada em %.1f cm\n", v);
    }
    return true;
}

// ---------- Tarefas ----------
// LÃª o que vocÃª digita no idf.py monitor (UART0). O monitor nÃ£o mostra o que
// vocÃª digita, entÃ£o os caracteres sÃ£o ecoados aqui.
static void tarefa_serial(void *arg)
{
    char linha[CMD_MAX];
    int n = 0;
    while (1) {
        char c;
        int r = read(STDIN_FILENO, &c, 1);
        if (r <= 0) {
            vTaskDelay(pdMS_TO_TICKS(20));
            continue;
        }
        if (c == '\r' || c == '\n') {
            if (n > 0) {
                linha[n] = '\0';
                printf("\n");
                fflush(stdout);
                enviar_comando(linha);
                n = 0;
            }
            continue;
        }
        if ((c == 8 || c == 127) && n > 0) {   // backspace
            n--;
            printf("\b \b");
            fflush(stdout);
            continue;
        }
        if (n < CMD_MAX - 1 && isprint((unsigned char)c)) {
            linha[n++] = c;
            putchar(c);
            fflush(stdout);
        }
    }
}

static void tarefa_app(void *arg)
{
    TickType_t ultimo_ciclo = xTaskGetTickCount();
    const TickType_t intervalo = pdMS_TO_TICKS(INTERVALO_MS);

    while (1) {
        TickType_t decorrido = xTaskGetTickCount() - ultimo_ciclo;
        TickType_t espera = decorrido >= intervalo ? 0 : intervalo - decorrido;

        comando_t cmd;
        if (xQueueReceive(fila_cmd, &cmd, espera) == pdTRUE) {
            if (aplicar_comando(cmd.texto)) {
                executar_ciclo();
                ultimo_ciclo = xTaskGetTickCount();
            }
        } else {
            executar_ciclo();
            ultimo_ciclo = xTaskGetTickCount();
        }
    }
}

// ---------- Wi-Fi ----------
static void wifi_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        ESP_LOGI(TAG, "[WiFi] Conectando a '%s'...", WIFI_SSID);
        esp_wifi_connect();
    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        wifi_event_sta_disconnected_t *ev = (wifi_event_sta_disconnected_t *)data;
        wifi_ok = false;
        ESP_LOGW(TAG, "[WiFi] Desconectado (motivo=%d: %s). Tentando de novo...",
                 ev->reason, motivo_wifi(ev->reason));
        esp_wifi_connect();
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *ev = (ip_event_got_ip_t *)data;
        ESP_LOGI(TAG, "[WiFi] OK - IP " IPSTR, IP2STR(&ev->ip_info.ip));
        wifi_ok = true;
    }
}

static void iniciar_wifi(void)
{
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_handler, NULL, NULL));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, wifi_handler, NULL, NULL));

    wifi_config_t wcfg = {0};
    strncpy((char *)wcfg.sta.ssid, WIFI_SSID, sizeof(wcfg.sta.ssid) - 1);
    strncpy((char *)wcfg.sta.password, WIFI_PASSWORD, sizeof(wcfg.sta.password) - 1);

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wcfg));
    ESP_ERROR_CHECK(esp_wifi_start());
}

// ---------- MQTT ----------
static void mqtt_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    esp_mqtt_event_handle_t ev = (esp_mqtt_event_handle_t)data;

    switch ((esp_mqtt_event_id_t)id) {
    case MQTT_EVENT_CONNECTED:
        mqtt_ok = true;
        ESP_LOGI(TAG, "[MQTT] Conectado como %s", client_id);
        esp_mqtt_client_publish(mqtt, T_CONEXAO, "ONLINE", 0, 1, 1);
        esp_mqtt_client_subscribe(mqtt, T_SIMULAR, 1);
        enviar_comando("!ciclo");
        break;

    case MQTT_EVENT_DISCONNECTED:
        mqtt_ok = false;
        ESP_LOGW(TAG, "[MQTT] Desconectado. A biblioteca reconecta sozinha.");
        break;

    case MQTT_EVENT_SUBSCRIBED:
        ESP_LOGI(TAG, "[MQTT] Inscrito em %s", T_SIMULAR);
        break;

    case MQTT_EVENT_DATA: {
        if (ev->topic_len == 0) break;   // fragmento de mensagem grande, ignora
        char topico[64] = {0};
        char msg[CMD_MAX] = {0};
        int tl = ev->topic_len < (int)sizeof(topico) - 1 ? ev->topic_len : (int)sizeof(topico) - 1;
        int dl = ev->data_len  < CMD_MAX - 1 ? ev->data_len  : CMD_MAX - 1;
        memcpy(topico, ev->topic, tl);
        memcpy(msg, ev->data, dl);
        ESP_LOGI(TAG, "[MQTT] Recebido em %s: %s", topico, msg);
        if (strcmp(topico, T_SIMULAR) == 0) enviar_comando(msg);
        break;
    }

    case MQTT_EVENT_ERROR:
        if (ev->error_handle->error_type == MQTT_ERROR_TYPE_TCP_TRANSPORT) {
            ESP_LOGE(TAG, "[MQTT] Erro de rede (errno=%d: %s). Broker inacessivel ou porta 1883 bloqueada?",
                     ev->error_handle->esp_transport_sock_errno,
                     strerror(ev->error_handle->esp_transport_sock_errno));
        } else if (ev->error_handle->error_type == MQTT_ERROR_TYPE_CONNECTION_REFUSED) {
            ESP_LOGE(TAG, "[MQTT] Broker recusou a conexao (codigo=%d)",
                     ev->error_handle->connect_return_code);
        } else {
            ESP_LOGE(TAG, "[MQTT] Erro tipo %d", ev->error_handle->error_type);
        }
        break;

    default:
        break;
    }
}

static void iniciar_mqtt(void)
{
    esp_mqtt_client_config_t cfg = {
        .broker.address.uri = BROKER_URI,
        .credentials.client_id = client_id,
        .session.keepalive = MQTT_KEEPALIVE_S,
        .session.last_will = {
            .topic  = T_CONEXAO,
            .msg    = "OFFLINE",
            .qos    = 1,
            .retain = 1,
        },
    };
    mqtt = esp_mqtt_client_init(&cfg);
    esp_mqtt_client_register_event(mqtt, ESP_EVENT_ANY_ID, mqtt_handler, NULL);
    ESP_ERROR_CHECK(esp_mqtt_client_start(mqtt));
}

// ---------- app_main ----------
void app_main(void)
{
    printf("\n=== SMART BIN - boot ===\n");

    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    const char *base = TOPIC_BASE;
    snprintf(T_NIVEL,   sizeof(T_NIVEL),   "%s/nivel",   base);
    snprintf(T_STATUS,  sizeof(T_STATUS),  "%s/status",  base);
    snprintf(T_ALERTA,  sizeof(T_ALERTA),  "%s/alerta",  base);
    snprintf(T_CONEXAO, sizeof(T_CONEXAO), "%s/conexao", base);
    snprintf(T_SIMULAR, sizeof(T_SIMULAR), "%s/simular", base);

    uint8_t mac[6];
    esp_read_mac(mac, ESP_MAC_WIFI_STA);
    snprintf(client_id, sizeof(client_id), "smartbin-%02x%02x%02x", mac[3], mac[4], mac[5]);

    fila_cmd = xQueueCreate(8, sizeof(comando_t));

    configurar_gpios();
    iniciar_wifi();

    // Espera o IP antes de iniciar o MQTT (LEDs e comandos jÃ¡ funcionam depois)
    ESP_LOGI(TAG, "Aguardando Wi-Fi...");
    while (!wifi_ok) vTaskDelay(pdMS_TO_TICKS(200));

    iniciar_mqtt();

    xTaskCreate(tarefa_serial, "serial", 3072, NULL, 5, NULL);
    xTaskCreate(tarefa_app,    "app",    4096, NULL, 5, NULL);

    mostrar_ajuda();
}
