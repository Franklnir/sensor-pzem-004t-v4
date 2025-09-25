/*
 * === ESP32-S3 Mini — PZEM-004T v3 + Firebase + BME280/BH1750 ===
 * Versi: MURNI TELEMETRY (RESET kWh JARAK JAUH) + RELAY 4-CH + SCHEDULER.
 *
 * Stream Firebase:
 *  - /users/{uid}/commands           (reset_kwh -> reset akumulator harian/bulanan)
 *  - /users/{uid}/relay_4_channel    (stream state + scheduler HH:MM & days)
 *
 * Penting wiring:
 *  - PZEM UART1: RX=11 (ke TX PZEM), TX=12 (ke RX PZEM)
 *  - ESP32-S3 relay pins default: 6,7,15,16 (ACTIVE-LOW) — ganti jika perlu
 */

#include <Arduino.h>
#include <math.h>
#include <Wire.h>
#include <time.h>
#include <sys/time.h> // settimeofday

// ===== Perangkat & WiFi (ESP32 saja) =====
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <WiFiMulti.h>     // <- multi-SSID

#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
#include <BH1750.h>
#include <PZEM004Tv30.h>

#define FIREBASE_DISABLE_SD
#include <Firebase_ESP_Client.h>
#include <addons/TokenHelper.h>
#include <addons/RTDBHelper.h>

// ================== KONFIGURASI WIFI & FIREBASE ==================
#define WIFI_SSID_1       "GEORGIA"
#define WIFI_PASSWORD_1   "Georgia12345"
#define WIFI_SSID_2       "Universitas Pelita Bangsa New"
#define WIFI_PASSWORD_2   "megah123"

#define API_KEY         "AIzaSyD69fUIQw6sKicbp3kbkqB114gMFonfklM"
#define DATABASE_URL    "https://projectrumah-6e924-default-rtdb.firebaseio.com/"
#define USER_EMAIL      "myfrankln@gmail.com"
#define USER_PASSWORD   "123456"

// ================== WAKTU (WIB) ==================
const long TZ_OFFSET = 7 * 3600; // UTC+7
const int  DST_OFFSET = 0;
const char* ntpServers[] = {"id.pool.ntp.org","pool.ntp.org","time.google.com","time.nist.gov"};

// ================== UART PZEM ==================
#define PZEM_RX_PIN 11  // TX PZEM -> RX ESP
#define PZEM_TX_PIN 12  // RX PZEM <- TX ESP
HardwareSerial PZEMSerial(1);
PZEM004Tv30 pzem(PZEMSerial, PZEM_RX_PIN, PZEM_TX_PIN);

// ================== Sensor Lingkungan (tanpa MQ135) ==================
#define SDA_PIN    9
#define SCL_PIN    8
#define BUZZER_PIN 4
#define LED_PIN    13

Adafruit_BME280 bme;
BH1750 lightMeter;

// ================== Firebase ==================
FirebaseData fbdo;
FirebaseData streamCmd;      // stream untuk /commands (reset_kwh)
FirebaseData streamRelays;   // stream untuk /relay_4_channel
FirebaseAuth auth;
FirebaseConfig config;

String gUID;                 // UID aktif

// ================== Relay (ACTIVE-LOW, ESP32) ==================
const int relayPins[4] = {6, 7, 5, 10};  // sesuaikan dg board kamu
bool lastRelayState[4] = {false, false, false, false};
const char* RELAY_LABELS[4] = {"relay 1","relay 2","relay 3","relay 4"};

// ================== Filter EMA (listrik) ==================
const float ALPHA = 0.25f;
struct Filtered { float v=NAN, i=NAN, p=NAN, e=NAN, f=NAN, pf=NAN, S=NAN, Q=NAN; } filt;
static inline float ema(float prev, float x){ if(isnan(prev)) return x; return prev + ALPHA*(x-prev); }
const float V_GAIN=1.0f, I_GAIN=1.0f, P_GAIN=1.0f;

// ================== Akumulator Energi (harian/bulanan) ==================
#include <Preferences.h>
Preferences prefs;
float today_kwh=0.0f, month_kwh=0.0f, lastE_kwh=NAN;
int lastDay=-1, lastMonth=-1, lastYear=-1;

// ================== Timer kirim ==================
unsigned long lastSendMs=0;
const unsigned long SEND_INTERVAL_MS=3000;
unsigned long lastPrint=0;
const unsigned long PRINT_EVERY_MS=1000;

// ================== Scheduler Ticker ==================
unsigned long lastScheduleTick = 0;
const unsigned long TICK_MS = 1000;
String lastMinuteChecked = "";

// ================== PATH RUNTIME untuk Relay ==================
String baseUserPath;
String baseRelaysPath;

// ================== WiFiMulti ==================
WiFiMulti wifiMulti;

// ================== Util waktu ==================
static bool isLeap(int y){ return ((y%4==0)&&(y%100!=0))||(y%400==0); }
static long daysBeforeYear(int y){ long d=0; for(int yr=1970; yr<y; ++yr) d+=365+(isLeap(yr)?1:0); return d; }
static int  daysBeforeMonth(int y,int m0){ static const int cum[12]={0,31,59,90,120,151,181,212,243,273,304,334}; int d=cum[m0]; if(m0>=2&&isLeap(y)) d+=1; return d; }
static time_t timegm_portable(const struct tm* tmv){
  int y=tmv->tm_year+1900, m=tmv->tm_mon, d=tmv->tm_mday;
  long long days=(long long)daysBeforeYear(y)+daysBeforeMonth(y,m)+(d-1);
  return (time_t)(days*86400LL + tmv->tm_hour*3600LL + tmv->tm_min*60LL + tmv->tm_sec);
}
static bool myGetLocalTime(struct tm* info, uint32_t ms=5000){
  time_t now; uint32_t t0=millis();
  while((millis()-t0)<=ms){ time(&now); if(now>1609459200){ localtime_r(&now, info); return true; } delay(10); yield(); }
  return false;
}
bool syncTimeFromHTTP(){
  WiFiClient client; const char* host="google.com"; if(!client.connect(host,80)) return false;
  client.print(String("HEAD / HTTP/1.1\r\nHost: ")+host+"\r\nConnection: close\r\n\r\n");
  uint32_t t0=millis();
  while(client.connected() && millis()-t0<3000){
    String line=client.readStringUntil('\n');
    if(line.startsWith("Date: ")){
      struct tm tmv{}; char wk[4],mon[4]; int d,y,H,M,S;
      if(sscanf(line.c_str(),"Date: %3s, %d %3s %d %d:%d:%d GMT",wk,&d,mon,&y,&H,&M,&S)==7){
        const char* months="JanFebMarAprMayJunJulAugSepOctNovDec";
        const char* p=strstr(months,mon); int m=p?((int)(p-months)/3):0;
        tmv.tm_year=y-1900; tmv.tm_mon=m; tmv.tm_mday=d; tmv.tm_hour=H; tmv.tm_min=M; tmv.tm_sec=S;
        time_t tutc=timegm_portable(&tmv); struct timeval now={tutc+TZ_OFFSET,0}; settimeofday(&now,nullptr);
        client.stop(); return true;
      }
    }
    if(line=="\r") break;
  }
  client.stop(); return false;
}
static inline void ensureTime(){
  Serial.print("Sinkronisasi waktu");
  bool ok=false;
  for(size_t i=0;i<sizeof(ntpServers)/sizeof(ntpServers[0]);++i){
    configTime(TZ_OFFSET, DST_OFFSET, ntpServers[i]);
    struct tm ti; unsigned long t0=millis();
    while(!myGetLocalTime(&ti,250) && (millis()-t0<3500)){ Serial.print("."); delay(250); }
    if(myGetLocalTime(&ti,1)){ ok=true; break; }
  }
  if(!ok){ Serial.print("... fallback HTTP time"); if(syncTimeFromHTTP()) ok=true; }
  Serial.println(ok? " -> OK":" -> GAGAL");
}
bool timeReady(){ time_t now=time(nullptr); return now > 24*3600; }

String tsWIB(){
  time_t now=time(nullptr);
  struct tm tm_info; localtime_r(&now, &tm_info);
  char buf[32]; strftime(buf,sizeof(buf),"%Y-%m-%d %H:%M:%S",&tm_info);
  return String(buf);
}
String isoTimeWIB(){
  time_t now=time(nullptr);
  struct tm tm_info; localtime_r(&now,&tm_info);
  char buf[32]; strftime(buf,sizeof(buf),"%Y-%m-%dT%H:%M:%S+07:00",&tm_info);
  return String(buf);
}
String now_HHMM(){ struct tm t; if(!myGetLocalTime(&t)) return ""; char b[6]; snprintf(b,sizeof(b),"%02d:%02d",t.tm_hour,t.tm_min); return String(b); }
String todayKey(){ struct tm t; if(!myGetLocalTime(&t)) return ""; const char* keys[7]={"sun","mon","tue","wed","thu","fri","sat"}; return String(keys[t.tm_wday]); }
int nowMinutes(){ struct tm t; if(!myGetLocalTime(&t)) return -1; return t.tm_hour*60 + t.tm_min; }
bool parseHHMM_toMinutes(const String& hhmm, int& outMin){
  if(hhmm.length()!=5 || hhmm.charAt(2)!=':') return false;
  int hh = hhmm.substring(0,2).toInt();
  int mm = hhmm.substring(3,5).toInt();
  if(hh<0||hh>23||mm<0||mm>59) return false;
  outMin = hh*60 + mm; return true;
}
bool isInWindow(const String& startHHMM, const String& endHHMM, int nowMin){
  int s,e;
  if(!parseHHMM_toMinutes(startHHMM, s) || !parseHHMM_toMinutes(endHHMM, e)) return true; // invalid -> bebas
  if(s==e) return true;              // 24 jam
  if(s<e)  return (nowMin >= s && nowMin < e);
  return (nowMin >= s) || (nowMin < e); // lintas tengah malam
}

// ================== Helper Cetak ==================
String fmtDualMilli(const char* longName, const char* shortUnit, float valueA){
  char buf[96];
  if (fabs(valueA) < 1.0f){
    float m = valueA * 1000.0f;
    snprintf(buf,sizeof(buf),"%0.3f %s (%s) = %0.1f milli%s (m%s)",valueA,longName,shortUnit,m,longName,shortUnit);
  } else {
    snprintf(buf,sizeof(buf),"%0.3f %s (%s)",valueA,longName,shortUnit);
  }
  return String(buf);
}
String fmtDualKilo(const char* longName, const char* shortUnit, float valueBase){
  char buf[96];
  if (fabs(valueBase) >= 1000.0f){
    float k = valueBase/1000.0f;
    snprintf(buf,sizeof(buf),"%0.1f %s (%s) = %0.3f kilo%s (k%s)",valueBase,longName,shortUnit,k,longName,shortUnit);
  } else {
    snprintf(buf,sizeof(buf),"%0.1f %s (%s)",valueBase,longName,shortUnit);
  }
  return String(buf);
}
String fmtEnergyBoth(float kWh){
  char buf[96];
  if (fabs(kWh) < 1.0f){
    float Wh = kWh*1000.0f;
    snprintf(buf,sizeof(buf),"%0.3f kilowatt-hour (kWh) = %0.1f watt-hour (Wh)",kWh,Wh);
  } else {
    snprintf(buf,sizeof(buf),"%0.3f kilowatt-hour (kWh)",kWh);
  }
  return String(buf);
}
void printDetailedReadout(float V,float I,float P,float E_kWh,float F,float PF,float S_VA,float Q_var,const char* ts){
  Serial.println("------------------------------------------------------");
  Serial.printf("⏱ Waktu (WIB)       : %s\n", ts);
  Serial.println("📊 Pembacaan (detail):");
  if (fabs(V) < 1.0f)       Serial.printf("  Tegangan            : %0.3f V = %0.1f mV\n", V, V*1000.0f);
  else if (fabs(V) >= 1000) Serial.printf("  Tegangan            : %0.1f V = %0.3f kV\n", V, V/1000.0f);
  else                      Serial.printf("  Tegangan            : %0.1f V\n", V);
  Serial.printf("  Arus                : %s\n", fmtDualMilli("Ampere","A",I).c_str());
  Serial.printf("  Daya Aktif          : %s\n", fmtDualKilo("Watt","W",P).c_str());
  Serial.printf("  Energi Akumulatif   : %s\n", fmtEnergyBoth(E_kWh).c_str());
  Serial.printf("  Frekuensi           : %0.1f Hz\n", F);
  Serial.printf("  Faktor Daya (cos φ) : %0.2f\n", PF);
  Serial.printf("  Daya Semu           : %s\n", fmtDualKilo("Volt–Ampere","VA",S_VA).c_str());
  Serial.printf("  Daya Reaktif        : %s\n", fmtDualKilo("volt–ampere reaktif","var",Q_var).c_str());
  Serial.println("\nℹ️  S = V×I,   P = S×PF,   Q = √(S² − P²)");
}

// ================== NVS helpers (energi bulanan) ==================
void loadMonthBaseline(){
  prefs.begin("pzem-bill", true);
  month_kwh = prefs.getFloat("month_kwh", 0.0f);
  lastYear  = prefs.getInt("year", -1);
  lastMonth = prefs.getInt("month",-1);
  prefs.end();
}
void saveMonthBaseline(float mkwh, int y, int m){
  prefs.begin("pzem-bill", false);
  prefs.putFloat("month_kwh", mkwh);
  prefs.putInt("year", y);
  prefs.putInt("month", m);
  prefs.end();
}

// ================== WiFi helpers (Multi SSID stabil) ==================
static inline void setupWifiMulti(){
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);                    // penting untuk stream TLS stabil
  WiFi.setTxPower(WIFI_POWER_19_5dBm);

  wifiMulti.addAP(WIFI_SSID_1, WIFI_PASSWORD_1);
  wifiMulti.addAP(WIFI_SSID_2, WIFI_PASSWORD_2);
}

static inline void waitForWifi(){
  setupWifiMulti();
  Serial.printf("Menghubungkan WiFi: \"%s\" lalu \"%s\" (fallback)\n", WIFI_SSID_1, WIFI_SSID_2);

  unsigned long tStart = millis();
  uint8_t st;
  do {
    st = wifiMulti.run();                  // ESP32 core 3.x: return uint8_t
    if (st == WL_CONNECTED) break;
    Serial.print(".");
    delay(500);
    if (millis() - tStart > 30000){
      Serial.println("\nRetry cari AP...");
      tStart = millis();
    }
  } while (st != WL_CONNECTED);

  Serial.printf("\nWiFi OK. SSID=%s IP=%s RSSI=%d dBm\n",
                WiFi.SSID().c_str(), WiFi.localIP().toString().c_str(), WiFi.RSSI());
}

// ================== Firebase Auth ==================
bool waitReadyWithUID(uint32_t timeout_ms){
  unsigned long t0=millis();
  while((!Firebase.ready() || auth.token.uid=="") && (millis()-t0<timeout_ms)){ delay(200); }
  if (auth.token.uid=="") return false;
  gUID = String(auth.token.uid.c_str());
  Serial.printf("🔑 UID: %s\n", gUID.c_str());
  return true;
}
void startFirebaseCommandStream(){
  if (gUID=="") return;
  String commandPath = "/users/" + gUID + "/commands";
  if (Firebase.RTDB.beginStream(&streamCmd, commandPath.c_str())){
    Serial.printf("✅ Listening commands: %s\n", commandPath.c_str());
  } else {
    Serial.printf("❌ Command stream error: %s\n", streamCmd.errorReason().c_str());
  }
}
bool initFirebaseAuth(){
  config.api_key = API_KEY;
  config.database_url = DATABASE_URL;
  config.token_status_callback = tokenStatusCallback;
  config.timeout.serverResponse = 15000;

  auth.user.email = USER_EMAIL;
  auth.user.password = USER_PASSWORD;

  Firebase.begin(&config, &auth);
  Firebase.reconnectWiFi(true);
  Serial.println("🔥 Sign-in Firebase...");
  if (waitReadyWithUID(15000)){ startFirebaseCommandStream(); return true; }

  Serial.println("⚠ Email/Password gagal -> Anonymous");
  auth.user.email.clear(); auth.user.password.clear();
  if (!Firebase.signUp(&config, &auth, "", "")){
    Serial.printf("❌ Anonymous signUp gagal: %s\n", config.signer.signupError.message.c_str());
    return false;
  }
  Firebase.begin(&config, &auth);
  Firebase.reconnectWiFi(true);
  if (waitReadyWithUID(15000)){ startFirebaseCommandStream(); return true; }
  return false;
}

// ================== RESET ENERGI ==================
void resetEnergyCounters(){
  Serial.println("\n!!! ================================= !!!");
  Serial.println("!!!      REMOTE RESET DITERIMA      !!!");
  Serial.println("!!! ================================= !!!");
  today_kwh = 0.0f; month_kwh = 0.0f;

  float currentEnergy = pzem.energy();
  if (!isnan(currentEnergy)){ lastE_kwh = currentEnergy; Serial.printf("Baseline energi baru: %.5f kWh\n", lastE_kwh); }
  else Serial.println("WARNING: Gagal baca PZEM saat reset, baseline tak diubah.");

  if (timeReady()){
    time_t nowt=time(nullptr);
    struct tm tm_info; localtime_r(&nowt,&tm_info);
    saveMonthBaseline(0.0f, tm_info.tm_year+1900, tm_info.tm_mon+1);
    Serial.println("Rekap bulanan (NVS) di-reset.");
  }

  String commandPath = "/users/" + gUID + "/commands/reset_kwh";
  if (Firebase.RTDB.deleteNode(&fbdo, commandPath.c_str())) Serial.println("✅ Command reset dihapus.");
  else Serial.printf("❌ Gagal hapus command: %s\n", fbdo.errorReason().c_str());
}

// ================== RELAY: aksi & ack ==================
void applyRelay(int idx, bool on){
  if(idx<0 || idx>=4) return;
  if(lastRelayState[idx]==on) return;
  lastRelayState[idx]=on;
  digitalWrite(relayPins[idx], on?LOW:HIGH); // ACTIVE-LOW
  Serial.printf("[RELAY] %s -> %s\n", RELAY_LABELS[idx], on? "ON":"OFF");
}
void writeStateToCloud(int idx, bool state){
  if(gUID=="") return;
  Firebase.RTDB.setBool(&fbdo, baseRelaysPath + "/" + String(idx) + "/state", state);
}
void ackMeta(int idx, const char* byTag){
  if(gUID=="") return;
  String base = baseRelaysPath + "/" + String(idx) + "/meta";
  Firebase.RTDB.setString(&fbdo, base + "/by", byTag);
  Firebase.RTDB.setInt(&fbdo,    base + "/ts", (int)time(nullptr));
}

// ================== RELAY: stream & scheduler helpers ==================
bool beginStreamWithRetry(const String& path, uint8_t maxTry=5){
  for(uint8_t i=0;i<maxTry;i++){
    if(Firebase.RTDB.beginStream(&streamRelays, path.c_str())){
      Serial.printf("Stream aktif di %s\n", path.c_str());
      return true;
    }
    Serial.printf("Gagal mulai stream (%d/%d): %s\n", i+1, maxTry, streamRelays.errorReason().c_str());
    delay(800);
  }
  return false;
}
void ensureStreamAlive(){
  if(!streamRelays.httpConnected() || streamRelays.httpCode()<=0){
    Serial.println("Stream relay tidak aktif, re-konek...");
    beginStreamWithRetry(baseRelaysPath);
  }
}
int parseIndexFromStatePath(const String& p){
  if(!p.startsWith("/")) return -1;
  int slash2 = p.indexOf('/',1); if(slash2<0) return -1;
  String mid = p.substring(1,slash2);
  String tail= p.substring(slash2+1);
  int idx = mid.toInt();
  return (tail=="state" && idx>=0 && idx<4)? idx : -1;
}

// ================== SETUP ==================
void setup(){
  Serial.begin(115200); delay(300);
  setCpuFrequencyMhz(80);
  Serial.printf("⚙  CPU set ke %d MHz\n", getCpuFrequencyMhz());

  // Relay pins default OFF (ACTIVE-LOW -> tulis HIGH)
  for(int i=0;i<4;i++){ pinMode(relayPins[i], OUTPUT); digitalWrite(relayPins[i], HIGH); }

  // I2C dan IO lain
  Wire.begin(SDA_PIN, SCL_PIN);
  Wire.setClock(100000);    // lebih santai & stabil
  Wire.setTimeOut(50);      // cegah hang
  pinMode(BUZZER_PIN, OUTPUT);
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(BUZZER_PIN, LOW);
  digitalWrite(LED_PIN, LOW);
  analogReadResolution(12);

  // WiFi & waktu
  waitForWifi();
  ensureTime();
  configTime(TZ_OFFSET, DST_OFFSET, "pool.ntp.org","time.google.com");

  // Sensor lingkungan
  Serial.println("\n🔎 Inisialisasi sensor lingkungan...");
  bool bmeOK = bme.begin(0x76);   // ganti ke 0x77 jika modulmu 0x77
  bool bhOK  = lightMeter.begin(BH1750::CONTINUOUS_HIGH_RES_MODE);
  Serial.printf("BME280 : %s\n", bmeOK ? "OK" : "TIDAK TERDETEKSI");
  Serial.printf("BH1750 : %s\n", bhOK ? "OK" : "TIDAK TERDETEKSI");
  if (!bmeOK || !bhOK) Serial.println("⚠ Sensor tidak lengkap. Nilai tertentu bisa NaN/0.");

  // PZEM UART
  Serial.println("\n=== PZEM Monitor (ESP32-S3 Mini) — Detail ===");
  Serial.printf("UART1: RX=%d  TX=%d\n", PZEM_RX_PIN, PZEM_TX_PIN);
  PZEMSerial.begin(9600, SERIAL_8N1, PZEM_RX_PIN, PZEM_TX_PIN);

  // Rekap energi tersimpan
  loadMonthBaseline();

  // Firebase Auth
  if (!initFirebaseAuth()){
    Serial.println("❌ Autentikasi Firebase gagal total.");
  }

  // Bangun path runtime
  if (gUID!=""){
    baseUserPath   = "/users/" + gUID;
    baseRelaysPath = baseUserPath + "/relay_4_channel";
  }

  // Seed /relay_4_channel/*/state & sinkronisasi pin
  for(int i=0;i<4;i++){
    String p = baseRelaysPath + "/" + String(i) + "/state";
    if(Firebase.RTDB.getBool(&fbdo, p)){
      applyRelay(i, fbdo.boolData());
    } else {
      Firebase.RTDB.setBool(&fbdo, p, false);
      applyRelay(i, false);
    }
  }

  // Mulai stream
  if (baseRelaysPath.length()) beginStreamWithRetry(baseRelaysPath);

  Serial.println("\n======================================================");
  Serial.println(" ESP32-S3 + PZEM + BME280/BH1750 + Relay + Scheduler");
  Serial.println(" RTDB Paths:");
  Serial.println("   /users/{uid}/sensor_lingkungan");
  Serial.println("   /users/{uid}/sensor_listrik");
  Serial.println("   /users/{uid}/commands");
  Serial.println("   /users/{uid}/relay_4_channel");
  Serial.println("======================================================\n");
}

// ================== LOOP ==================
void loop(){
  // Jaga WiFi (auto pilih AP terbaik + reconnect halus)
  if (wifiMulti.run() != WL_CONNECTED){
    Serial.println("📡 WiFi putus. Reconnect...");
  }

  // ======= 1) Stream /commands: reset_kwh =======
  if (Firebase.ready() && streamCmd.streamAvailable()){
    if (streamCmd.dataPath()=="/reset_kwh"){
      String asStr = streamCmd.stringData();
      bool trigger = (asStr=="true") || streamCmd.boolData();
      if (trigger) resetEnergyCounters();
    }
  }

  // ======= 2) Stream /relay_4_channel =======
  if(Firebase.RTDB.readStream(&streamRelays)){
    if(streamRelays.dataAvailable()){
      if(streamRelays.dataTypeEnum()==fb_esp_rtdb_data_type_boolean){
        String p = streamRelays.dataPath(); // contoh: "/0/state"
        int idx = parseIndexFromStatePath(p);
        if(idx >= 0){
          bool v = streamRelays.boolData();
          if(v != lastRelayState[idx]){
            applyRelay(idx, v);
            writeStateToCloud(idx, v);
            ackMeta(idx, "app");
            Serial.printf("[CMD] %s => %s\n", RELAY_LABELS[idx], v? "ON":"OFF");
          }
        }
      }
    }
  } else {
    if(streamRelays.httpCode()!=0){
      Serial.printf("Stream relay error (%d): %s\n", streamRelays.httpCode(), streamRelays.errorReason().c_str());
    }
    ensureStreamAlive();
  }

  // ======= 3) Scheduler HH:MM & days (cek tiap menit) =======
  unsigned long nowMs = millis();
  if(nowMs - lastScheduleTick >= TICK_MS){
    lastScheduleTick = nowMs;
    String hhmm = now_HHMM();
    if(hhmm.length() && hhmm != lastMinuteChecked){
      lastMinuteChecked = hhmm;

      String today = todayKey(); // "sun","mon",...
      int nowMin = nowMinutes();

      for(int i=0;i<4;i++){
        String chBase = baseRelaysPath + "/" + String(i);

        bool todayEnabled = true;
        if(Firebase.RTDB.getBool(&fbdo, chBase + "/days/" + today)){ todayEnabled = fbdo.boolData(); }
        if(!todayEnabled) continue;

        String onStr, offStr;
        bool haveOn=false, haveOff=false;
        if(Firebase.RTDB.getString(&fbdo, chBase + "/waktuON"))  { onStr  = fbdo.stringData(); haveOn=true; }
        if(Firebase.RTDB.getString(&fbdo, chBase + "/waktuOFF")) { offStr = fbdo.stringData(); haveOff=true; }

        if(haveOn && haveOff){
          bool shouldBeOn = isInWindow(onStr, offStr, nowMin);
          if(shouldBeOn && !lastRelayState[i]){
            applyRelay(i, true);
            writeStateToCloud(i, true);
            ackMeta(i, "schedule_hold");
            Serial.printf("[AUTO] %s HOLD ON (now=%s window %s-%s)\n", RELAY_LABELS[i], hhmm.c_str(), onStr.c_str(), offStr.c_str());
          } else if(!shouldBeOn && lastRelayState[i]){
            applyRelay(i, false);
            writeStateToCloud(i, false);
            ackMeta(i, "schedule_hold");
            Serial.printf("[AUTO] %s HOLD OFF (now=%s window %s-%s)\n", RELAY_LABELS[i], hhmm.c_str(), onStr.c_str(), offStr.c_str());
          }
        } else {
          if(haveOn  && onStr  == hhmm && !lastRelayState[i]){
            applyRelay(i, true);  writeStateToCloud(i, true);  ackMeta(i, "schedule");
            Serial.printf("[AUTO] %s ON  @ %s\n", RELAY_LABELS[i], hhmm.c_str());
          }
          if(haveOff && offStr == hhmm &&  lastRelayState[i]){
            applyRelay(i, false); writeStateToCloud(i, false); ackMeta(i, "schedule");
            Serial.printf("[AUTO] %s OFF @ %s\n", RELAY_LABELS[i], hhmm.c_str());
          }
        }
      }
    }
  }

  // ======= 4) Baca PZEM & hitung ringkasan harian/bulanan =======
  float v  = pzem.voltage();
  float i  = pzem.current();
  float p  = pzem.power();
  float e  = pzem.energy();
  float f  = pzem.frequency();
  float pf = pzem.pf();

  if (!(isnan(v)||isnan(i)||isnan(p)||isnan(e)||isnan(f)||isnan(pf))){
    v *= V_GAIN; i *= I_GAIN; p *= P_GAIN;
    float S = v*i;
    float P = p;
    float Q = NAN;
    if (pf > 0 && pf <= 1.0f){
      float S2=S*S, P2=P*P; Q = (S2>P2)? sqrtf(S2-P2):0.0f;
    }

    filt.v  = ema(filt.v, v);
    filt.i  = ema(filt.i, i);
    filt.p  = ema(filt.p, P);
    filt.e  = ema(filt.e, e);
    filt.f  = ema(filt.f, f);
    filt.pf = ema(filt.pf, pf);
    filt.S  = ema(filt.S, S);
    filt.Q  = ema(filt.Q, Q);

    time_t nowt=time(nullptr);
    struct tm tm_info; localtime_r(&nowt,&tm_info);

    if (timeReady()){
      int y=tm_info.tm_year+1900, m=tm_info.tm_mon+1, d=tm_info.tm_mday;
      if (lastDay==-1){ lastYear=y; lastMonth=m; lastDay=d; lastE_kwh=e; }
      if (m!=lastMonth || y!=lastYear){
        saveMonthBaseline(month_kwh, lastYear, lastMonth);
        month_kwh=0.0f; lastMonth=m; lastYear=y; lastE_kwh=e;
      }
      if (d!=lastDay){ today_kwh=0.0f; lastDay=d; lastE_kwh=e; }
      float delta = e - lastE_kwh;
      if (delta>0 && delta<100.0f){ today_kwh+=delta; month_kwh+=delta; lastE_kwh=e; }
    } else {
      if (isnan(lastE_kwh)) lastE_kwh=e;
      float delta = e - lastE_kwh;
      if (delta>0 && delta<100.0f){ today_kwh+=delta; month_kwh+=delta; lastE_kwh=e; }
    }

    if (millis()-lastPrint > PRINT_EVERY_MS){
      lastPrint = millis();
      printDetailedReadout(filt.v,filt.i,filt.p,e,filt.f,filt.pf,filt.S,filt.Q, tsWIB().c_str());
      Serial.printf("\n[Ringkasan Energi] Harian: %s | Bulanan: %s\n",
                    fmtEnergyBoth(today_kwh).c_str(),
                    fmtEnergyBoth(month_kwh).c_str());
    }
  } else {
    if (millis()-lastPrint > PRINT_EVERY_MS){
      lastPrint = millis();
      Serial.println("NaN: cek TX/RX silang, GND, Vcc 5V, dan level shifter (jika perlu).");
    }
  }

  // ======= 5) BME280/BH1750 + kirim RTDB tiap 3 detik =======
  unsigned long nowMs2 = millis();
  if (nowMs2 - lastSendMs >= SEND_INTERVAL_MS){
    lastSendMs = nowMs2;

    float suhu=NAN, kelembapan=NAN, tekanan=NAN, altitude=NAN, lightLevel=NAN;
    if (bme.sensorID()) {
      suhu       = bme.readTemperature();
      kelembapan = bme.readHumidity();
      tekanan    = bme.readPressure()/100.0f;
      altitude   = bme.readAltitude(1013.25);
    }
    lightLevel = lightMeter.readLightLevel();

    String ts = isoTimeWIB();

    FirebaseJson jsonEnv;
    jsonEnv.set("timestamp", ts);
    if (!isnan(suhu))       jsonEnv.set("suhu",        suhu);
    if (!isnan(kelembapan)) jsonEnv.set("kelembapan",  kelembapan);
    if (!isnan(tekanan))    jsonEnv.set("tekanan",     tekanan);
    if (!isnan(altitude))   jsonEnv.set("altitude",    altitude);
    if (!isnan(lightLevel)) jsonEnv.set("light_level", lightLevel);

    FirebaseJson jsonListrik;
    jsonListrik.set("timestamp", ts);
    if(!isnan(filt.v))  jsonListrik.set("tegangan_V",        filt.v);
    if(!isnan(filt.i))  jsonListrik.set("arus_A",            filt.i);
    if(!isnan(filt.p))  jsonListrik.set("daya_aktif_W",      filt.p);
    if(!isnan(filt.f))  jsonListrik.set("frekuensi_Hz",      filt.f);
    if(!isnan(filt.pf)) jsonListrik.set("faktor_daya",       filt.pf);
    if(!isnan(filt.S))  jsonListrik.set("daya_semu_VA",      filt.S);
    if(!isnan(filt.Q))  jsonListrik.set("daya_reaktif_var",  filt.Q);
    jsonListrik.set("energi_total_kWh",   pzem.energy());
    jsonListrik.set("energi_harian_kWh",  today_kwh);
    jsonListrik.set("energi_bulanan_kWh", month_kwh);

    Serial.println("------------------------------------------------------");
    Serial.printf("⏱  %s\n", ts.c_str());
    Serial.printf("UID         : %s\n", gUID.c_str());
    Serial.printf("WiFi RSSI   : %d dBm\n", WiFi.RSSI());
    Serial.printf("Buzzer      : %s\n", (digitalRead(BUZZER_PIN)==HIGH) ? "ON" : "OFF");

    bool okEnv=false, okListrik=false;
    if (Firebase.ready() && gUID!=""){
      okEnv     = Firebase.RTDB.setJSON(&fbdo, ("/users/"+gUID+"/sensor_lingkungan").c_str(), &jsonEnv);
      okListrik = Firebase.RTDB.setJSON(&fbdo, ("/users/"+gUID+"/sensor_listrik").c_str(),   &jsonListrik);
    } else {
      Serial.println("⏳ Firebase belum siap atau UID kosong.");
    }

    if (okEnv || okListrik){ digitalWrite(LED_PIN, HIGH); delay(50); digitalWrite(LED_PIN, LOW); }
    if (okEnv)     Serial.println("✅ Firebase: data lingkungan terkirim.");
    else           Serial.printf("❌ Firebase (lingkungan) gagal: %s\n", fbdo.errorReason().c_str());
    if (okListrik) Serial.println("✅ Firebase: data listrik terkirim.");
    else           Serial.printf("❌ Firebase (listrik) gagal: %s\n", fbdo.errorReason().c_str());
  }

  delay(200);
}
