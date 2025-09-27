/* ====== ESP32-S3 Mini — PZEM004T v3 + BME280/BH1750 + Firebase RTDB + Relay ======
 * Versi: Diperbarui dengan Saran Akurasi (Trimmed Mean & AutoCal Guards)
 *
 * FOKUS UTAMA: Stabilitas & Akurasi
 * - I2CGuard + EnvSensors auto-recover (BME280/BH1750)
 * - RelayGuard (debounce, rate-limit, stagger, hold-off setelah WiFi)
 * - PZEM akurasi TINGGI:
 * • Multi-sampling (5x 'trimmed mean') V/I/P/PF/F/E -> Lebih stabil dari median
 * • Blended Power: gabung P_sensor & V*I*PF berdasar mismatch
 * • Auto-zero I (idle) + clamp range
 * • Kalibrasi cepat V/I dari Firebase: /users/<uid>/calib/vi_ref {"v":..,"i":..}
 * • AutoCal energi 2-titik dari meter PLN (dengan guard waktu & delta energi)
 * ------------------------------------------------------------------------------- */

#include <Arduino.h>
#include <math.h>
#include <Wire.h>
#include <time.h>
#include <sys/time.h>

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <WiFiMulti.h>

#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
#include <BH1750.h>
#include <PZEM004Tv30.h>

#define FIREBASE_DISABLE_SD
#include <Firebase_ESP_Client.h>
#include <addons/TokenHelper.h>
#include <addons/RTDBHelper.h>

#include <Preferences.h>

// ================== KONFIG WIFI & FIREBASE ==================
#define WIFI_SSID_1      "GEORGIA"
#define WIFI_PASSWORD_1  "Georgia12345"
#define WIFI_SSID_2      "Universitas Pelita Bangsa New"
#define WIFI_PASSWORD_2  "megah123"

#define API_KEY          "AIzaSyD69fUIQw6sKicbp3kbkqB114gMFonfklM"
#define DATABASE_URL     "https://projectrumah-6e924-default-rtdb.firebaseio.com/"
#define USER_EMAIL       "myfrankln@gmail.com"
#define USER_PASSWORD    "123456"

// ================== WAKTU (WIB) ==================
const long TZ_OFFSET = 7 * 3600; // UTC+7
const int  DST_OFFSET = 0;
const char* ntpServers[] = {"id.pool.ntp.org","pool.ntp.org","time.google.com","time.nist.gov"};

// ================== UART PZEM ==================
// PENTING: Pastikan catu daya 5V ke modul PZEM stabil dan bersih!
#define PZEM_RX_PIN 11
#define PZEM_TX_PIN 12
HardwareSerial PZEMSerial(1);
PZEM004Tv30 pzem(PZEMSerial, PZEM_RX_PIN, PZEM_TX_PIN);

// ================== Sensor Lingkungan & IO ==================
#define SDA_PIN    9
#define SCL_PIN    8
#define BUZZER_PIN 4
#define LED_PIN    13

Adafruit_BME280 bme;
BH1750 lightMeter;

// ================== Firebase ==================
FirebaseData fbdo;
FirebaseData streamCmd;      // /commands
FirebaseData streamRelays;   // /relay_4_channel
FirebaseAuth auth;
FirebaseConfig config;
String gUID;

// ================== Relay (ACTIVE-LOW) ==================
const int relayPins[4] = {6, 7, 5, 10};
bool lastRelayState[4] = {false, false, false, false};
const char* RELAY_LABELS[4] = {"relay 1","relay 2","relay 3","relay 4"};

// ================== Filter EMA ==================
const float ALPHA = 0.25f;
struct Filtered { float v=NAN, i=NAN, p=NAN, e=NAN, f=NAN, pf=NAN, S=NAN, Q=NAN; } filt;
static inline float ema(float prev, float x){ if(isnan(prev)) return x; return prev + ALPHA*(x-prev); }

// ================== Akumulator Energi ==================
Preferences prefs;
float today_kwh=0.0f, month_kwh=0.0f, lastE_kwh=NAN;
int lastDay=-1, lastMonth=-1, lastYear=-1;

// ================== Kalibrasi (NVS) ==================
struct Calib {
  float v_gain = 1.000f, v_off = 0.000f;
  float i_gain = 1.000f, i_off = 0.000f;
  float p_gain = 1.000f, p_off = 0.000f;    // skala power sensor
  float kwh_gain = 1.000f;                  // skala energi kumulatif
} calib;

void loadCalib() {
  prefs.begin("pzem-calib", true);
  calib.v_gain = prefs.getFloat("v_gain", 1.0f);
  calib.v_off  = prefs.getFloat("v_off",  0.0f);
  calib.i_gain = prefs.getFloat("i_gain", 1.0f);
  calib.i_off  = prefs.getFloat("i_off",  0.0f);
  calib.p_gain = prefs.getFloat("p_gain", 1.0f);
  calib.p_off  = prefs.getFloat("p_off",  0.0f);
  calib.kwh_gain = prefs.getFloat("kwh_gain", 1.0f);
  prefs.end();
}
void saveCalib() {
  prefs.begin("pzem-calib", false);
  prefs.putFloat("v_gain", calib.v_gain);
  prefs.putFloat("v_off",  calib.v_off);
  prefs.putFloat("i_gain", calib.i_gain);
  prefs.putFloat("i_off",  calib.i_off);
  prefs.putFloat("p_gain", calib.p_gain);
  prefs.putFloat("p_off",  calib.p_off);
  prefs.putFloat("kwh_gain", calib.kwh_gain);
  prefs.end();
}

// ================== Timer ==================
unsigned long lastSendMs=0;
const unsigned long SEND_INTERVAL_MS=3000;
unsigned long lastPrint=0;
const unsigned long PRINT_EVERY_MS=1000;

// ================== Scheduler ==================
unsigned long lastScheduleTick = 0;
const unsigned long TICK_MS = 1000;
String lastMinuteChecked = "";

// ================== PATH Firebase ==================
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
  WiFiClient client; const char* host="google.com";
  if(!client.connect(host,80)) return false;
  client.print(String("HEAD / HTTP/1.1\r\nHost: ") + host + "\r\nConnection: close\r\n\r\n");
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
String tsWIB(){ time_t now=time(nullptr); struct tm tm_info; localtime_r(&now, &tm_info); char buf[32]; strftime(buf,sizeof(buf),"%Y-%m-%d %H:%M:%S",&tm_info); return String(buf); }
String isoTimeWIB(){ time_t now=time(nullptr); struct tm tm_info; localtime_r(&now,&tm_info); char buf[32]; strftime(buf,sizeof(buf),"%Y-%m-%dT%H:%M:%S+07:00",&tm_info); return String(buf); }
String now_HHMM(){ struct tm t; if(!myGetLocalTime(&t)) return ""; char b[6]; snprintf(b,sizeof(b),"%02d:%02d",t.tm_hour,t.tm_min); return String(b); }
String todayKey(){ struct tm t; if(!myGetLocalTime(&t)) return ""; const char* keys[7]={"sun","mon","tue","wed","thu","fri","sat"}; return String(keys[t.tm_wday]); }
int nowMinutes(){ struct tm t; if(!myGetLocalTime(&t)) return -1; return t.tm_hour*60 + t.tm_min; }
bool parseHHMM_toMinutes(const String& hhmm, int& outMin){ if(hhmm.length()!=5 || hhmm.charAt(2)!=':') return false; int hh = hhmm.substring(0,2).toInt(); int mm = hhmm.substring(3,5).toInt(); if(hh<0||hh>23||mm<0||mm>59) return false; outMin = hh*60 + mm; return true; }
bool isInWindow(const String& startHHMM, const String& endHHMM, int nowMin){
  int s,e; if(!parseHHMM_toMinutes(startHHMM, s) || !parseHHMM_toMinutes(endHHMM, e)) return true;
  if(s==e) return true; if(s<e) return (nowMin >= s && nowMin < e); return (nowMin >= s) || (nowMin < e);
}

// ================== Util cetak ==================
static inline void printDetailedReadout(float V,float I,float P,float E,float F,float PF,float S,float Q,const char* ts){
  Serial.printf("[%s] V=%.2fV I=%.3fA P=%.1fW S=%.1fVA Q=%.1fvar PF=%.3f F=%.2fHz E=%.5fkWh\n",
    ts, V,I,P,S,Q,PF,F,E);
}

// ================== NVS helpers (energi bulanan) ==================
void loadMonthBaseline(){ prefs.begin("pzem-bill", true); month_kwh = prefs.getFloat("month_kwh", 0.0f); lastYear  = prefs.getInt("year", -1); lastMonth = prefs.getInt("month",-1); prefs.end(); }
void saveMonthBaseline(float mkwh, int y, int m){ prefs.begin("pzem-bill", false); prefs.putFloat("month_kwh", mkwh); prefs.putInt("year", y); prefs.putInt("month", m); prefs.end(); }

// ================== WiFi helpers ==================
static inline void setupWifiMulti(){ WiFi.mode(WIFI_STA); WiFi.setSleep(false); WiFi.setTxPower(WIFI_POWER_19_5dBm); wifiMulti.addAP(WIFI_SSID_1, WIFI_PASSWORD_1); wifiMulti.addAP(WIFI_SSID_2, WIFI_PASSWORD_2); }
static inline void waitForWifi(){
  setupWifiMulti();
  Serial.printf("Menghubungkan WiFi: \"%s\" lalu \"%s\" (fallback)\n", WIFI_SSID_1, WIFI_SSID_2);
  unsigned long tStart = millis(); uint8_t st;
  do { st = wifiMulti.run(); if (st == WL_CONNECTED) break; Serial.print("."); delay(500);
       if (millis() - tStart > 30000){ Serial.println("\nRetry cari AP..."); tStart = millis(); } } while (st != WL_CONNECTED);
  Serial.printf("\nWiFi OK. SSID=%s IP=%s RSSI=%d dBm\n", WiFi.SSID().c_str(), WiFi.localIP().toString().c_str(), WiFi.RSSI());
}

// ================== Firebase Auth ==================
bool waitReadyWithUID(uint32_t timeout_ms){
  unsigned long t0=millis(); while((!Firebase.ready() || auth.token.uid=="") && (millis()-t0<timeout_ms)){ delay(200); yield(); }
  if (auth.token.uid=="") return false; gUID = String(auth.token.uid.c_str()); Serial.printf("🔑 UID: %s\n", gUID.c_str()); return true;
}
void startFirebaseCommandStream(){
  if (gUID=="") return; String commandPath = "/users/" + gUID + "/commands";
  if (Firebase.RTDB.beginStream(&streamCmd, commandPath.c_str())){ Serial.printf("✅ Listening commands: %s\n", commandPath.c_str()); }
  else { Serial.printf("❌ Command stream error: %s\n", streamCmd.errorReason().c_str()); }
}
bool initFirebaseAuth(){
  config.api_key = API_KEY; config.database_url = DATABASE_URL; config.token_status_callback = tokenStatusCallback; config.timeout.serverResponse = 12000;
  auth.user.email = USER_EMAIL; auth.user.password = USER_PASSWORD;
  Firebase.begin(&config, &auth); Firebase.reconnectWiFi(true); Serial.println("🔥 Sign-in Firebase...");
  if (waitReadyWithUID(15000)){ startFirebaseCommandStream(); return true; }
  Serial.println("⚠ Email/Password gagal -> Anonymous"); auth.user.email.clear(); auth.user.password.clear();
  if (!Firebase.signUp(&config, &auth, "", "")){ Serial.printf("❌ Anonymous signUp gagal: %s\n", config.signer.signupError.message.c_str()); return false; }
  Firebase.begin(&config, &auth); Firebase.reconnectWiFi(true);
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
    time_t nowt=time(nullptr); struct tm tm_info; localtime_r(&nowt,&tm_info);
    saveMonthBaseline(0.0f, tm_info.tm_year+1900, tm_info.tm_mon+1);
    Serial.println("Rekap bulanan (NVS) di-reset.");
  }
  String commandPath = "/users/" + gUID + "/commands/reset_kwh";
  if (Firebase.RTDB.deleteNode(&fbdo, commandPath.c_str())) Serial.println("✅ Command reset dihapus.");
  else Serial.printf("❌ Gagal hapus command: %s\n", fbdo.errorReason().c_str());
}

// ================== RELAY GUARD ==================
struct RelayGuard {
  unsigned long lastChangeMs[4] = {0,0,0,0};
  unsigned long lastAnyChangeMs = 0;
  const unsigned long debounceMs = 120;
  const unsigned long minPerChanGapMs = 180;
  const unsigned long minAnyGapMs    = 120;
  bool wifiJustReconn = false; unsigned long wifiReconnAt = 0;
  void markWifiReconn(){ wifiJustReconn = true; wifiReconnAt = millis(); }
  bool holdoffAfterWifi(){ if(!wifiJustReconn) return false; if(millis()-wifiReconnAt>300) {wifiJustReconn=false; return false;} return true; }
  bool canToggle(int idx){
    unsigned long now = millis();
    if (holdoffAfterWifi()) return false;
    if (now - lastAnyChangeMs < minAnyGapMs) return false;
    if (now - lastChangeMs[idx] < max(debounceMs,minPerChanGapMs)) return false;
    return true;
  }
  void apply(int idx, bool on){
    if(idx<0||idx>=4) return;
    if(lastRelayState[idx]==on) return;
    if(!canToggle(idx)) return;
    unsigned long sinceAny = millis()-lastAnyChangeMs;
    if (sinceAny < minAnyGapMs) delay(minAnyGapMs - sinceAny);
    lastRelayState[idx]=on;
    digitalWrite(relayPins[idx], on?LOW:HIGH);
    lastChangeMs[idx]=millis(); lastAnyChangeMs=millis();
    Serial.printf("[RELAY] %s -> %s\n", RELAY_LABELS[idx], on? "ON":"OFF");
    digitalWrite(LED_PIN, HIGH); delay(30); digitalWrite(LED_PIN, LOW);
  }
} rguard;

void applyRelay(int idx, bool on){ rguard.apply(idx,on); }
void writeStateToCloud(int idx, bool state){ if(gUID=="") return; Firebase.RTDB.setBool(&fbdo, baseRelaysPath + "/" + String(idx) + "/state", state); }
void ackMeta(int idx, const char* byTag){ if(gUID=="") return; String base = baseRelaysPath + "/" + String(idx) + "/meta"; Firebase.RTDB.setString(&fbdo, base + "/by", byTag); Firebase.RTDB.setInt(&fbdo, base + "/ts", (int)time(nullptr)); }
bool beginStreamWithRetry(const String& path, uint8_t maxTry=5){
  for(uint8_t i=0;i<maxTry;i++){
    if(Firebase.RTDB.beginStream(&streamRelays, path.c_str())){ Serial.printf("Stream aktif di %s\n", path.c_str()); return true; }
    Serial.printf("Gagal mulai stream (%d/%d): %s\n", i+1, maxTry, streamRelays.errorReason().c_str()); delay(800);
  }
  return false;
}
void ensureStreamAlive(){ if(!streamRelays.httpConnected() || streamRelays.httpCode()<=0){ Serial.println("Stream relay tidak aktif, re-konek..."); beginStreamWithRetry(baseRelaysPath); } }
int parseIndexFromStatePath(const String& p){ if(!p.startsWith("/")) return -1; int slash2 = p.indexOf('/',1); if(slash2<0) return -1; String mid = p.substring(1,slash2); String tail= p.substring(slash2+1); int idx = mid.toInt(); return (tail=="state" && idx>=0 && idx<4)? idx : -1; }

// ================== Statistik Kuat & Pembacaan PZEM ==================
struct PZEMRead { float v=NAN,i=NAN,p=NAN,e=NAN,f=NAN,pf=NAN; bool ok=false; };

// BARU: Fungsi Trimmed Mean untuk menggantikan Median
// Mengurutkan 5 sampel, membuang nilai terendah & tertinggi, lalu merata-ratakan 3 sisanya.
// Ini memberikan stabilitas rata-rata sambil tetap kebal terhadap outlier.
static float trimmedMean3of5(float a, float b, float c, float d, float e) {
    float arr[5] = {a, b, c, d, e};
    // Insertion sort kecil (efisien untuk N kecil)
    for (int i = 1; i < 5; i++) {
        float key = arr[i];
        int j = i - 1;
        while (j >= 0 && arr[j] > key) {
            arr[j + 1] = arr[j];
            j--;
        }
        arr[j + 1] = key;
    }
    // Kembalikan rata-rata dari 3 nilai tengah (arr[1], arr[2], arr[3])
    return (arr[1] + arr[2] + arr[3]) / 3.0f;
}

// MODIFIKASI: Menggunakan trimmedMean3of5 untuk stabilitas pembacaan yang lebih tinggi
PZEMRead robustReadPZEM5(uint16_t gap_ms=80){
  float v[5], i[5], p[5], e[5], f[5], pf[5];
  for(int k=0;k<5;k++){
    v[k]=pzem.voltage();
    i[k]=pzem.current();
    p[k]=pzem.power();
    e[k]=pzem.energy();
    f[k]=pzem.frequency();
    pf[k]=pzem.pf();
    delay(gap_ms);
  }
  float v_stable = trimmedMean3of5(v[0],v[1],v[2],v[3],v[4]);
  float i_stable = trimmedMean3of5(i[0],i[1],i[2],i[3],i[4]);
  float p_stable = trimmedMean3of5(p[0],p[1],p[2],p[3],p[4]);
  float e_stable = trimmedMean3of5(e[0],e[1],e[2],e[3],e[4]);
  float f_stable = trimmedMean3of5(f[0],f[1],f[2],f[3],f[4]);
  float pf_stable= trimmedMean3of5(pf[0],pf[1],pf[2],pf[3],pf[4]);

  PZEMRead R;
  R.v  = v_stable;
  R.i  = i_stable;
  R.p  = p_stable;
  R.e  = e_stable;
  R.f  = f_stable;
  R.pf = pf_stable;
  
  R.ok = !(isnan(R.v)||isnan(R.i)||isnan(R.p)||isnan(R.e)||isnan(R.f)||isnan(R.pf));
  return R;
}

// ================== Auto-zero & Meter PLN Auto-tune ==================
const float ZERO_I_MAX_A = 0.10f;
const uint32_t ZERO_WINDOW_MS = 6000;
const float I_OFF_MIN = -0.20f;
const float I_OFF_MAX =  0.00f;
uint32_t zeroStartMs = 0; bool zeroWindowActive = false;

static inline bool allRelaysOff(){ for(int i=0;i<4;i++) if (lastRelayState[i]) return false; return true; }
void autoZeroCurrentIfIdle(float i_inst) {
  if (allRelaysOff() && !isnan(i_inst) && fabsf(i_inst) <= ZERO_I_MAX_A) {
    if (!zeroWindowActive) { zeroWindowActive = true; zeroStartMs = millis(); }
    if (zeroWindowActive && (millis() - zeroStartMs >= ZERO_WINDOW_MS)) {
      float new_i_off = constrain(-i_inst, I_OFF_MIN, I_OFF_MAX);
      if (fabsf(new_i_off - calib.i_off) > 0.002f) { calib.i_off = new_i_off; saveCalib(); Serial.printf("🤖 Auto-zero: set i_off = %.3f A (idle=%.3f A)\n", calib.i_off, i_inst); }
      zeroWindowActive = false;
    }
  } else { zeroWindowActive = false; }
}

// --- Referensi meter PLN (dua titik) ---
struct MeterRef { double kwh = NAN; uint32_t ts = 0; };
MeterRef refA, refB; double snapA_e = NAN, snapB_e = NAN;

void loadMeterRefs(){ prefs.begin("meter-ref", true);
  refA.kwh = prefs.getDouble("A_kwh", NAN); refA.ts  = prefs.getUInt("A_ts", 0);
  refB.kwh = prefs.getDouble("B_kwh", NAN); refB.ts  = prefs.getUInt("B_ts", 0);
  snapA_e  = prefs.getDouble("A_e", NAN);   snapB_e  = prefs.getDouble("B_e", NAN);
  prefs.end();
}
void saveMeterRefs(){ prefs.begin("meter-ref", false);
  prefs.putDouble("A_kwh", refA.kwh); prefs.putUInt("A_ts", refA.ts);
  prefs.putDouble("B_kwh", refB.kwh); prefs.putUInt("B_ts", refB.ts);
  prefs.putDouble("A_e", snapA_e);     prefs.putDouble("B_e", snapB_e);
  prefs.end();
}

uint32_t lastPollMeterMs = 0;
const uint32_t POLL_METER_MS = 7000;

// MODIFIKASI: Ditambahkan guard untuk membuat auto-kalibrasi lebih robust
void tryApplyEnergyGainFromRefs(){
  if (isnan(refA.kwh) || isnan(refB.kwh) || isnan(snapA_e) || isnan(snapB_e)) return;
  if (refA.ts <= refB.ts) return; // Titik A harus lebih baru dari B

  // GUARD BARU 1: Pastikan jeda waktu antar referensi cukup panjang
  const uint32_t MIN_CALIB_SECONDS = 3600 * 6; // Minimal 6 jam
  if (refA.ts - refB.ts < MIN_CALIB_SECONDS) {
      Serial.println("⚠ Auto-Calib: Jeda waktu antar referensi terlalu singkat (<6 jam), kalibrasi diabaikan.");
      return;
  }

  double dRef  = refA.kwh  - refB.kwh;
  double dPzem = snapA_e   - snapB_e;
  
  // GUARD BARU 2: Pastikan konsumsi energi cukup signifikan untuk dihitung
  const double MIN_ENERGY_DELTA_KWH = 0.5; // Minimal 0.5 kWh
  if (dRef <= MIN_ENERGY_DELTA_KWH || dPzem <= MIN_ENERGY_DELTA_KWH) {
      Serial.println("⚠ Auto-Calib: Delta energi terlalu kecil (<0.5 kWh), kalibrasi diabaikan.");
      return;
  }
  
  // guard loncatan energi (misal counter reset)
  if (dPzem < 0) { Serial.println("⚠ PZEM energy snapshot mundur — abaikan kalibrasi."); return; }

  double gain = dRef / dPzem; if (gain < 0.90) gain = 0.90; if (gain > 1.10) gain = 1.10;
  calib.p_gain   *= (float)gain; calib.kwh_gain *= (float)gain; saveCalib();
  Serial.printf("🤖 Auto-tune energi: dRef=%.3f kWh dPzem=%.3f kWh -> gain=%.4f\n", dRef, dPzem, gain);
  Serial.printf("    p_gain=%.4f  kwh_gain=%.4f (tersimpan)\n", calib.p_gain, calib.kwh_gain);
  refB.kwh = NAN; snapB_e = NAN; saveMeterRefs(); // Hapus titik B agar tidak dipakai lagi
}

void pollAndIngestMeterRef(){
  if (!Firebase.ready() || gUID=="") return;
  if (millis() - lastPollMeterMs < POLL_METER_MS) return;
  lastPollMeterMs = millis();

  // AutoCal Energi (dua titik)
  {
    String path = "/users/" + gUID + "/calib/meter_push";
    if (Firebase.RTDB.getJSON(&fbdo, path.c_str())) {
      FirebaseJson j = fbdo.jsonObject();
      FirebaseJsonData res;
      double kwhRef = NAN; int tsRef = 0;
      if (j.get(res, F("/kwh"))) { kwhRef = res.to<double>(); }
      if (j.get(res, F("/ts")))  { tsRef  = (int)res.to<int>(); }
      if (!isnan(kwhRef) && tsRef > 0) {
        refB = refA; snapB_e = snapA_e;
        refA.kwh = kwhRef; refA.ts  = (uint32_t)tsRef; snapA_e = pzem.energy();
        saveMeterRefs();
        Serial.printf("📥 MeterRef push: kWh=%.3f ts=%u | snapshot E=%.5f kWh\n", refA.kwh, refA.ts, snapA_e);
        Firebase.RTDB.deleteNode(&fbdo, path.c_str());
        tryApplyEnergyGainFromRefs();
      }
    }
  }

  // Kalibrasi cepat V/I (absolute reference)
  {
    String path = "/users/" + gUID + "/calib/vi_ref";
    if (Firebase.RTDB.getJSON(&fbdo, path.c_str())) {
      FirebaseJson j = fbdo.jsonObject(); FirebaseJsonData res;
      float v_ref=NAN, i_ref=NAN;
      if (j.get(res, F("/v"))) v_ref = res.to<float>();
      if (j.get(res, F("/i"))) i_ref = res.to<float>();

      PZEMRead r = robustReadPZEM5(60);
      if (!isnan(v_ref) && v_ref>80 && v_ref<280 && !isnan(r.v) && r.v>50){
        float new_gain = v_ref / r.v;
        calib.v_gain = constrain(new_gain, 0.90f, 1.10f);
        Serial.printf("🔧 Calib V: raw=%.2f ref=%.2f -> v_gain=%.4f\n", r.v, v_ref, calib.v_gain);
      }
      if (!isnan(i_ref) && i_ref>=0 && i_ref<100 && !isnan(r.i) && r.i>=0){
        float new_gain = (r.i>0.01f)? (i_ref / r.i) : calib.i_gain;
        calib.i_gain = constrain(new_gain, 0.90f, 1.20f);
        Serial.printf("🔧 Calib I: raw=%.3f ref=%.3f -> i_gain=%.4f\n", r.i, i_ref, calib.i_gain);
      }
      saveCalib();
      Firebase.RTDB.deleteNode(&fbdo, path.c_str());
    }
  }
}

// ================== I2C GUARD & ENV SENSORS ==================
struct I2CGuard {
  bool scanAddr(uint8_t addr){ Wire.beginTransmission(addr); return (Wire.endTransmission()==0); }
  void busRecover(){
    pinMode(SDA_PIN, INPUT_PULLUP); pinMode(SCL_PIN, INPUT_PULLUP);
    for(int i=0;i<9;i++){ pinMode(SCL_PIN, OUTPUT); delayMicroseconds(5); digitalWrite(SCL_PIN, LOW); delayMicroseconds(5);
                          pinMode(SCL_PIN, INPUT_PULLUP); delayMicroseconds(5); }
    pinMode(SDA_PIN, OUTPUT); digitalWrite(SDA_PIN, HIGH); delayMicroseconds(5);
    pinMode(SCL_PIN, OUTPUT); digitalWrite(SCL_PIN, HIGH); delayMicroseconds(5);
    Wire.begin(SDA_PIN, SCL_PIN); Wire.setClock(100000); Wire.setTimeOut(50);
  }
} i2cguard;

struct EnvSensors {
  bool bmeOK=false, bhOK=false; uint8_t bmeAddr=0x76;
  unsigned failCnt=0; unsigned long lastGoodMs=0, lastInitMs=0;
  const unsigned maxFail=8; const unsigned long reinitEveryMs=600000; const unsigned long staleMs=120000;
  bool begin(){
    lastInitMs=millis();
    if(!i2cguard.scanAddr(bmeAddr)){ Serial.println("I2C: BME280 tak terdeteksi, recovery bus..."); i2cguard.busRecover(); }
    bmeOK = bme.begin(bmeAddr);
    if(!bmeOK){ bmeAddr=0x77; if(!i2cguard.scanAddr(bmeAddr)) i2cguard.busRecover(); bmeOK = bme.begin(bmeAddr); }
    bhOK  = lightMeter.begin(BH1750::CONTINUOUS_HIGH_RES_MODE);
    if(!bhOK){ lightMeter.configure(BH1750::CONTINUOUS_HIGH_RES_MODE); bhOK = i2cguard.scanAddr(0x23) || i2cguard.scanAddr(0x5C); }
    Serial.printf("BME280 : %s (addr 0x%02X)\n", bmeOK?"OK":"FAIL", bmeAddr);
    Serial.printf("BH1750 : %s\n", bhOK?"OK":"FAIL");
    failCnt=0; return (bmeOK||bhOK);
  }
  void autoRecover(){
    unsigned long now=millis();
    if (failCnt>=maxFail){ Serial.println("I2C: gagal beruntun — RECOVER bus & reinit sensors"); i2cguard.busRecover(); begin(); }
    else if ((now-lastInitMs)>reinitEveryMs){ Serial.println("I2C: reinit berkala (prevent lock-up)"); begin(); }
    else if (lastGoodMs>0 && (now-lastGoodMs)>staleMs){ Serial.println("I2C: bacaan stale — reinit"); begin(); }
  }
  bool read(float &t, float &h, float &p_hpa, float &alt, float &lux){
    bool any=false, ok=true;
    if (bmeOK){
      float tt = bme.readTemperature();
      float hh = bme.readHumidity();
      float pp = bme.readPressure();
      if (isnan(tt) || isnan(hh) || isnan(pp)) { ok=false; failCnt++; }
      else { t=tt; h=hh; p_hpa=pp/100.0f; alt=bme.readAltitude(1013.25f); any=true; }
    }
    if (bhOK){
      float lx = lightMeter.readLightLevel();
      if (isnan(lx) || lx<0) { ok=false; failCnt++; }
      else { lux=lx; any=true; }
    }
    if (ok && any){ failCnt=0; lastGoodMs=millis(); }
    if (!ok) autoRecover();
    return any;
  }
} env;

// ================== SETUP ==================
void setup(){
  Serial.begin(115200); delay(300);
  setCpuFrequencyMhz(80);
  Serial.printf("⚙  CPU set ke %d MHz\n", getCpuFrequencyMhz());

  for(int i=0;i<4;i++){ pinMode(relayPins[i], OUTPUT); digitalWrite(relayPins[i], HIGH); }
  pinMode(BUZZER_PIN, OUTPUT); pinMode(LED_PIN, OUTPUT);
  digitalWrite(BUZZER_PIN, LOW); digitalWrite(LED_PIN, LOW);
  analogReadResolution(12);

  Wire.begin(SDA_PIN, SCL_PIN); Wire.setClock(100000); Wire.setTimeOut(50);

  waitForWifi(); ensureTime();
  configTime(TZ_OFFSET, DST_OFFSET, "pool.ntp.org","time.google.com");

  Serial.println("\n🔎 Inisialisasi sensor lingkungan (dengan guard)...");
  env.begin();

  Serial.println("\n=== PZEM Monitor (ESP32-S3 Mini) — Detail ===");
  Serial.printf("UART1: RX=%d  TX=%d\n", PZEM_RX_PIN, PZEM_TX_PIN);
  // PENTING: Untuk akurasi, pastikan CT Clamp hanya menjepit SATU kabel (Fasa/Live),
  // panah mengarah ke beban, dan clamp tertutup rapat tanpa celah.
  PZEMSerial.begin(9600, SERIAL_8N1, PZEM_RX_PIN, PZEM_TX_PIN);

  loadMonthBaseline(); loadCalib(); loadMeterRefs();
  Serial.printf("🔧 Calib awal: V(%.4f, %.3f) I(%.4f, %.3f) P(%.4f, %.3f) kWhGain=%.4f\n",
    calib.v_gain,calib.v_off,calib.i_gain,calib.i_off,calib.p_gain,calib.p_off,calib.kwh_gain);

  if (!initFirebaseAuth()){ Serial.println("❌ Autentikasi Firebase gagal total."); }
  if (gUID!=""){ baseUserPath = "/users/" + gUID; baseRelaysPath = baseUserPath + "/relay_4_channel"; }

  for(int i=0;i<4;i++){
    String p = baseRelaysPath + "/" + String(i) + "/state";
    if(Firebase.RTDB.getBool(&fbdo, p)){ applyRelay(i, fbdo.boolData()); }
    else { Firebase.RTDB.setBool(&fbdo, p, false); applyRelay(i, false); }
    delay(120);
  }
  if (baseRelaysPath.length()) beginStreamWithRetry(baseRelaysPath);

  Serial.println("\n======================================================");
  Serial.println(" ESP32-S3 + PZEM + BME280/BH1750 + Relay + Scheduler + Calib + AutoCal v5 (Stabilitas Tinggi)");
  Serial.println("======================================================\n");
}

// ================== LOOP ==================
void loop(){
  if (wifiMulti.run() != WL_CONNECTED){ Serial.println("📡 WiFi putus. Reconnect..."); rguard.markWifiReconn(); }

  pollAndIngestMeterRef();

  // ======= 1) Stream /commands =======
  if (Firebase.ready() && streamCmd.streamAvailable()){
    String pth = streamCmd.dataPath();
    if (pth == "/reset_kwh"){
      String asStr = streamCmd.stringData(); bool trigger = (asStr=="true") || streamCmd.boolData();
      if (trigger) resetEnergyCounters();
    } else if (pth == "/calibrate"){
      FirebaseJson j = streamCmd.jsonObject(); FirebaseJsonData res;
      if (j.get(res, F("/v_gain"))) calib.v_gain = res.to<float>();
      if (j.get(res, F("/v_off")))  calib.v_off  = res.to<float>();
      if (j.get(res, F("/i_gain"))) calib.i_gain = res.to<float>();
      if (j.get(res, F("/i_off")))  calib.i_off  = res.to<float>();
      if (j.get(res, F("/p_gain"))) calib.p_gain = res.to<float>();
      if (j.get(res, F("/p_off")))  calib.p_off  = res.to<float>();
      if (j.get(res, F("/kwh_gain"))) calib.kwh_gain = res.to<float>();
      saveCalib();
      Serial.printf("🔧 Calib updated: V(%.4f, %.3f) I(%.4f, %.3f) P(%.4f, %.3f) kWhGain=%.4f\n",
        calib.v_gain,calib.v_off,calib.i_gain,calib.i_off,calib.p_gain,calib.p_off,calib.kwh_gain);
      Firebase.RTDB.deleteNode(&fbdo, ("/users/"+gUID+"/commands/calibrate").c_str());
    }
  }

  // ======= 2) Stream /relay_4_channel =======
  if(Firebase.RTDB.readStream(&streamRelays)){
    if(streamRelays.dataAvailable()){
      if(streamRelays.dataTypeEnum()==fb_esp_rtdb_data_type_boolean){
        String p = streamRelays.dataPath();
        int idx = parseIndexFromStatePath(p);
        if(idx >= 0){
          bool v = streamRelays.boolData();
          if(v != lastRelayState[idx]){
            applyRelay(idx, v); writeStateToCloud(idx, v); ackMeta(idx, "app");
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

  // ======= 3) Scheduler =======
  unsigned long nowMs = millis();
  if(nowMs - lastScheduleTick >= TICK_MS){
    lastScheduleTick = nowMs;
    String hhmm = now_HHMM();
    if(hhmm.length() && hhmm != lastMinuteChecked){
      lastMinuteChecked = hhmm;
      String today = todayKey(); int nowMin = nowMinutes();
      for(int i=0;i<4;i++){
        String chBase = baseRelaysPath + "/" + String(i);
        bool todayEnabled = true;
        if(Firebase.RTDB.getBool(&fbdo, chBase + "/days/" + today)){ todayEnabled = fbdo.boolData(); }
        if(!todayEnabled) continue;
        String onStr, offStr; bool haveOn=false, haveOff=false;
        if(Firebase.RTDB.getString(&fbdo, chBase + "/waktuON"))  { onStr  = fbdo.stringData(); haveOn=true; }
        if(Firebase.RTDB.getString(&fbdo, chBase + "/waktuOFF")) { offStr = fbdo.stringData(); haveOff=true; }
        if(haveOn && haveOff){
          bool shouldBeOn = isInWindow(onStr, offStr, nowMin);
          if(shouldBeOn && !lastRelayState[i]){ applyRelay(i, true); writeStateToCloud(i, true); ackMeta(i, "schedule_hold"); }
          else if(!shouldBeOn && lastRelayState[i]){ applyRelay(i, false); writeStateToCloud(i, false); ackMeta(i, "schedule_hold"); }
        } else {
          if(haveOn  && onStr  == hhmm && !lastRelayState[i]){ applyRelay(i, true);  writeStateToCloud(i, true);  ackMeta(i, "schedule"); }
          if(haveOff && offStr == hhmm &&  lastRelayState[i]){ applyRelay(i, false); writeStateToCloud(i, false); ackMeta(i, "schedule"); }
        }
        delay(20);
      }
    }
  }

  // ======= 4) PZEM + ringkasan energi (AKURAT) =======
  {
    PZEMRead r = robustReadPZEM5(80);
    if (r.ok){
      // Terapkan kalibrasi dasar
      float V  = r.v  * calib.v_gain + calib.v_off;
      float I  = r.i  * calib.i_gain + calib.i_off;
      float Pm = r.p  * calib.p_gain + calib.p_off; // power dari PZEM (meter)
      float E  = r.e;   // energy total dari PZEM (nanti dikoreksi via kwh_gain)
      float F  = r.f;
      float PF = constrain(r.pf, 0.0f, 1.0f);

      // Hitung power by formula
      float Pcalc = V * I * PF;
      // Blend: jika mismatch besar, beri bobot lebih ke Pcalc
      float mismatch = (Pm>1.0f) ? fabsf(Pm - Pcalc)/Pm : 0.0f; // relatif
      mismatch = constrain(mismatch, 0.0f, 1.0f);
      float w_sensor = 1.0f - constrain((mismatch - 0.05f)/0.30f, 0.0f, 1.0f); // toleransi 5%, full shift di ~35%
      float P = w_sensor*Pm + (1.0f - w_sensor)*Pcalc;

      // Daya semu & reaktif
      float S = V * I;
      float Q = NAN; if (PF > 0 && PF <= 1.0f){ float S2=S*S, P2=P*P; Q = (S2>P2)? sqrtf(S2-P2):0.0f; }

      // EMA filter
      filt.v=ema(filt.v,V); filt.i=ema(filt.i,I); filt.p=ema(filt.p,P); filt.e=ema(filt.e,E);
      filt.f=ema(filt.f,F); filt.pf=ema(filt.pf,PF); filt.S=ema(filt.S,S); filt.Q=ema(filt.Q,Q);

      // Auto-zero arus saat idle
      autoZeroCurrentIfIdle(filt.i);

      // Akumulator energi (pakai delta E * kwh_gain)
      time_t nowt=time(nullptr); struct tm tm_info; localtime_r(&nowt,&tm_info);
      auto apply_delta = [&](float delta){
        if (delta>0 && delta < 0.50f){
          float delta_corr = delta * calib.kwh_gain;
          today_kwh += delta_corr; month_kwh += delta_corr; lastE_kwh = E;
        }
      };
      if (timeReady()){
        int y=tm_info.tm_year+1900, m=tm_info.tm_mon+1, d=tm_info.tm_mday;
        if (lastDay==-1){ lastYear=y; lastMonth=m; lastDay=d; lastE_kwh=E; }
        if (m!=lastMonth || y!=lastYear){ saveMonthBaseline(month_kwh, lastYear, lastMonth); month_kwh=0.0f; lastMonth=m; lastYear=y; lastE_kwh=E; }
        if (d!=lastDay){ today_kwh=0.0f; lastDay=d; lastE_kwh=E; }
        float delta = E - lastE_kwh;
        if (delta < 0) { // counter reset proteksi
          lastE_kwh = E;
        } else {
          apply_delta(delta);
        }
      } else {
        if (isnan(lastE_kwh)) lastE_kwh=E;
        float delta = E - lastE_kwh;
        if (delta >= 0) apply_delta(delta);
        else lastE_kwh=E;
      }

      if (millis()-lastPrint > PRINT_EVERY_MS){
        lastPrint = millis();
        printDetailedReadout(filt.v,filt.i,filt.p,E,filt.f,filt.pf,filt.S,filt.Q, tsWIB().c_str());
        Serial.printf("\n[Ringkasan Energi] Harian: %0.3f kWh | Bulanan: %0.3f kWh | gain(kWh)=%.4f Pblend(w_sensor=%.2f)\n",
                      today_kwh, month_kwh, calib.kwh_gain, w_sensor);
      }
    } else {
      if (millis()-lastPrint > PRINT_EVERY_MS){ lastPrint = millis();
        Serial.println("NaN: cek TX/RX silang, GND, 5V, dan pastikan CT hanya lewat SATU konduktor (L)."); }
    }
  }

  // ======= 4b) Baca SENSOR LINGKUNGAN (guarded) =======
  float suhu=NAN, kelembapan=NAN, tekanan=NAN, altitude=NAN, lightLevel=NAN;
  env.read(suhu, kelembapan, tekanan, altitude, lightLevel);

  // ======= 5) Kirim RTDB =======
  unsigned long nowMs2 = millis();
  if (nowMs2 - lastSendMs >= SEND_INTERVAL_MS){
    lastSendMs = nowMs2;
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
    if(!isnan(filt.v))  jsonListrik.set("tegangan_V",       filt.v);
    if(!isnan(filt.i))  jsonListrik.set("arus_A",           filt.i);
    if(!isnan(filt.p))  jsonListrik.set("daya_aktif_W",     filt.p);
    if(!isnan(filt.f))  jsonListrik.set("frekuensi_Hz",     filt.f);
    if(!isnan(filt.pf)) jsonListrik.set("faktor_daya",      filt.pf);
    if(!isnan(filt.S))  jsonListrik.set("daya_semu_VA",     filt.S);
    if(!isnan(filt.Q))  jsonListrik.set("daya_reaktif_var", filt.Q);
    jsonListrik.set("energi_total_kWh",   pzem.energy()); // raw (tanpa koreksi)
    jsonListrik.set("energi_harian_kWh",  today_kwh);     // sudah dikoreksi kwh_gain
    jsonListrik.set("energi_bulanan_kWh", month_kwh);     // sudah dikoreksi kwh_gain

    Serial.println("------------------------------------------------------");
    Serial.printf("⏱  %s\n", ts.c_str());
    Serial.printf("UID         : %s\n", gUID.c_str());
    Serial.printf("WiFi RSSI   : %d dBm\n", WiFi.RSSI());
    Serial.printf("Buzzer      : %s\n", (digitalRead(BUZZER_PIN)==HIGH) ? "ON" : "OFF");

    bool okEnv=false, okListrik=false;
    if (Firebase.ready() && gUID!=""){
      okEnv     = Firebase.RTDB.setJSON(&fbdo, ("/users/"+gUID+"/sensor_lingkungan").c_str(), &jsonEnv);
      okListrik = Firebase.RTDB.setJSON(&fbdo, ("/users/"+gUID+"/sensor_listrik").c_str(),    &jsonListrik);
    } else { Serial.println("⏳ Firebase belum siap atau UID kosong."); }

    if (okEnv || okListrik){ digitalWrite(LED_PIN, HIGH); delay(50); digitalWrite(LED_PIN, LOW); }
    if (okEnv)      Serial.println("✅ Firebase: data lingkungan terkirim.");
    else            Serial.printf("❌ Firebase (lingkungan) gagal: %s\n", fbdo.errorReason().c_str());
    if (okListrik) Serial.println("✅ Firebase: data listrik terkirim.");
    else            Serial.printf("❌ Firebase (listrik) gagal: %s\n", fbdo.errorReason().c_str());
  }

  delay(5);
}
