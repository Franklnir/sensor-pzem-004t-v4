/* ============================================================================
 * ESP32-S3 Mini — PZEM004T v3 + BME280/BH1750 + Supabase (REST) + Relay
 * Versi: Pure Supabase (tanpa Firebase)
 *
 * Fitur:
 * - Akurasi PZEM tinggi (trimmed mean 5x, blend Pm & Pcalc, auto-zero I, auto-cal energi 2 titik)
 * - BME280 + BH1750 dengan I2C Guard (auto-recover)
 * - Relay 4 channel (active low) dengan RelayGuard (debounce + rate limit) + scheduler harian
 * - Supabase:
 *   • monitoring_log  : simpan riwayat monitoring
 *   • relay_channel   : status relay + jadwal
 *   • device_commands : remote command (reset_kwh, calibrate, vi_ref, meter_push)
 * - LED notif: nyala 1 detik tiap kali data berhasil dikirim ke Supabase
 * ========================================================================== */

#include <Arduino.h>
#include <math.h>
#include <Wire.h>
#include <time.h>
#include <sys/time.h>

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <WiFiMulti.h>
#include <HTTPClient.h>

#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
#include <BH1750.h>
#include <PZEM004Tv30.h>

#include <Preferences.h>
#include <ArduinoJson.h>

// ================== KONFIG WIFI ==================
#define WIFI_SSID_1      "GEORGIA"
#define WIFI_PASSWORD_1  "Georgia12345"
#define WIFI_SSID_2      "Universitas Pelita Bangsa New"
#define WIFI_PASSWORD_2  "megah123"

// ================== KONFIG SUPABASE ==================
#define SUPABASE_URL      "https://zxwvsffndxwqfbldgvzp.supabase.co"
#define SUPABASE_API_KEY  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inp4d3ZzZmZuZHh3cWZibGRndnpwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjMzOTk1OTAsImV4cCI6MjA3ODk3NTU5MH0.CX8TSSI4PTIuBdJJXNJ9Uplf2BiXt4Qb0Y6REcr9pJo"

#define DEVICE_ID         "ESP32-S3-Monitoring-01"
#define TABLE_LOG         "monitoring_log"
#define TABLE_RELAY       "relay_channel"
#define TABLE_CMD         "device_commands"

// ================== WAKTU (WIB) ==================
const long TZ_OFFSET = 7 * 3600; // UTC+7
const int  DST_OFFSET = 0;
const char* ntpServers[] = {
  "id.pool.ntp.org","pool.ntp.org","time.google.com","time.nist.gov"
};

// ================== UART PZEM ==================
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

// ================== Relay (ACTIVE-LOW) ==================
const int relayPins[4] = {6, 7, 5, 10};
bool lastRelayState[4] = {false, false, false, false};
const char* RELAY_LABELS[4] = {"relay 1","relay 2","relay 3","relay 4"};

// ================== Filter EMA ==================
const float ALPHA = 0.25f;
struct Filtered {
  float v=NAN, i=NAN, p=NAN, e=NAN, f=NAN, pf=NAN, S=NAN, Q=NAN;
} filt;
static inline float ema(float prev, float x){
  if(isnan(prev)) return x;
  return prev + ALPHA*(x-prev);
}

// ================== Akumulator Energi & Kalibrasi ==================
Preferences prefs;
float today_kwh=0.0f, month_kwh=0.0f, lastE_kwh=NAN;
int lastDay=-1, lastMonth=-1, lastYear=-1;

struct Calib {
  float v_gain = 1.000f, v_off = 0.000f;
  float i_gain = 1.000f, i_off = 0.000f;
  float p_gain = 1.000f, p_off = 0.000f;
  float kwh_gain = 1.000f;
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

// ================== WiFiMulti ==================
WiFiMulti wifiMulti;

// ================== Util waktu ==================
static bool isLeap(int y){ return ((y%4==0)&&(y%100!=0))||(y%400==0); }
static long daysBeforeYear(int y){
  long d=0; for(int yr=1970; yr<y; ++yr) d+=365+(isLeap(yr)?1:0); return d;
}
static int  daysBeforeMonth(int y,int m0){
  static const int cum[12]={0,31,59,90,120,151,181,212,243,273,304,334};
  int d=cum[m0]; if(m0>=2&&isLeap(y)) d+=1; return d;
}
static time_t timegm_portable(const struct tm* tmv){
  int y=tmv->tm_year+1900, m=tmv->tm_mon, d=tmv->tm_mday;
  long long days=(long long)daysBeforeYear(y)+daysBeforeMonth(y,m)+(d-1);
  return (time_t)(days*86400LL + tmv->tm_hour*3600LL + tmv->tm_min*60LL + tmv->tm_sec);
}
static bool myGetLocalTime(struct tm* info, uint32_t ms=5000){
  time_t now; uint32_t t0=millis();
  while((millis()-t0)<=ms){
    time(&now);
    if(now>1609459200){ localtime_r(&now, info); return true; }
    delay(10); yield();
  }
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
    while(!myGetLocalTime(&ti,250) && (millis()-t0<3500)){
      Serial.print("."); delay(250);
    }
    if(myGetLocalTime(&ti,1)){ ok=true; break; }
  }
  if(!ok){
    Serial.print("... fallback HTTP time");
    if(syncTimeFromHTTP()) ok=true;
  }
  Serial.println(ok? " -> OK":" -> GAGAL");
}
bool timeReady(){
  time_t now=time(nullptr);
  return now > 24*3600;
}
String tsWIB(){
  time_t now=time(nullptr); struct tm tm_info;
  localtime_r(&now, &tm_info);
  char buf[32]; strftime(buf,sizeof(buf),"%Y-%m-%d %H:%M:%S",&tm_info);
  return String(buf);
}
String isoTimeWIB(){
  time_t now=time(nullptr); struct tm tm_info;
  localtime_r(&now,&tm_info);
  char buf[32]; strftime(buf,sizeof(buf),"%Y-%m-%dT%H:%M:%S+07:00",&tm_info);
  return String(buf);
}
String now_HHMM(){
  struct tm t; if(!myGetLocalTime(&t)) return "";
  char b[6]; snprintf(b,sizeof(b),"%02d:%02d",t.tm_hour,t.tm_min);
  return String(b);
}
int nowMinutes(){
  struct tm t; if(!myGetLocalTime(&t)) return -1;
  return t.tm_hour*60 + t.tm_min;
}
int dayIndex(){
  struct tm t; if(!myGetLocalTime(&t)) return 0;
  return t.tm_wday; // 0=Sun..6=Sat
}
bool parseHHMM_toMinutes(const String& hhmm, int& outMin){
  if(hhmm.length()<5) return false;
  int hh = hhmm.substring(0,2).toInt();
  int mm = hhmm.substring(3,5).toInt();
  if(hh<0||hh>23||mm<0||mm>59) return false;
  outMin = hh*60 + mm; return true;
}
bool isInWindow(const String& startHHMM, const String& endHHMM, int nowMin){
  int s,e; if(!parseHHMM_toMinutes(startHHMM, s) || !parseHHMM_toMinutes(endHHMM, e)) return true;
  if(s==e) return true;
  if(s<e)  return (nowMin >= s && nowMin < e);
  return (nowMin >= s) || (nowMin < e); // window lintas tengah malam
}

// ================== Util cetak ==================
static inline void printDetailedReadout(float V,float I,float P,float E,float F,float PF,float S,float Q,const char* ts){
  Serial.printf("[%s] V=%.2fV I=%.3fA P=%.1fW S=%.1fVA Q=%.1fvar PF=%.3f F=%.2fHz E=%.5fkWh\n",
    ts, V,I,P,S,Q,PF,F,E);
}

// ================== NVS helpers energi bulanan ==================
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

// ================== WiFi helpers ==================
static inline void setupWifiMulti(){
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.setTxPower(WIFI_POWER_19_5dBm);
  wifiMulti.addAP(WIFI_SSID_1, WIFI_PASSWORD_1);
  wifiMulti.addAP(WIFI_SSID_2, WIFI_PASSWORD_2);
}
static inline void waitForWifi(){
  setupWifiMulti();
  Serial.printf("Menghubungkan WiFi: \"%s\" lalu \"%s\" (fallback)\n", WIFI_SSID_1, WIFI_SSID_2);
  unsigned long tStart = millis(); uint8_t st;
  do {
    st = wifiMulti.run();
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

// ================== RESET ENERGI (local) ==================
void resetEnergyCountersLocal(){
  Serial.println("\n!!! ================================= !!!");
  Serial.println("!!!      RESET ENERGI (LOCAL)        !!!");
  Serial.println("!!! ================================= !!!");
  today_kwh = 0.0f;
  month_kwh = 0.0f;
  float currentEnergy = pzem.energy();
  if (!isnan(currentEnergy)){
    lastE_kwh = currentEnergy;
    Serial.printf("Baseline energi baru: %.5f kWh\n", lastE_kwh);
  } else {
    Serial.println("WARNING: Gagal baca PZEM saat reset, baseline tak diubah.");
  }
  if (timeReady()){
    time_t nowt=time(nullptr); struct tm tm_info;
    localtime_r(&nowt,&tm_info);
    saveMonthBaseline(0.0f, tm_info.tm_year+1900, tm_info.tm_mon+1);
    Serial.println("Rekap bulanan (NVS) di-reset.");
  }
}

// ================== RELAY GUARD ==================
struct RelayGuard {
  unsigned long lastChangeMs[4] = {0,0,0,0};
  unsigned long lastAnyChangeMs = 0;
  const unsigned long debounceMs = 120;
  const unsigned long minPerChanGapMs = 180;
  const unsigned long minAnyGapMs    = 120;
  bool wifiJustReconn = false;
  unsigned long wifiReconnAt = 0;

  void markWifiReconn(){ wifiJustReconn = true; wifiReconnAt = millis(); }
  bool holdoffAfterWifi(){
    if(!wifiJustReconn) return false;
    if(millis()-wifiReconnAt>300) {wifiJustReconn=false; return false;}
    return true;
  }
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
    lastChangeMs[idx]=millis();
    lastAnyChangeMs=millis();
    Serial.printf("[RELAY] %s -> %s\n", RELAY_LABELS[idx], on? "ON":"OFF");
    digitalWrite(LED_PIN, HIGH); delay(30); digitalWrite(LED_PIN, LOW);
  }
} rguard;

void applyRelay(int idx, bool on){ rguard.apply(idx,on); }

// ================== Statistik PZEM ==================
struct PZEMRead {
  float v=NAN,i=NAN,p=NAN,e=NAN,f=NAN,pf=NAN; bool ok=false;
};

static float trimmedMean3of5(float a, float b, float c, float d, float e) {
  float arr[5] = {a, b, c, d, e};
  for (int i = 1; i < 5; i++) {
    float key = arr[i];
    int j = i - 1;
    while (j >= 0 && arr[j] > key) {
      arr[j + 1] = arr[j];
      j--;
    }
    arr[j + 1] = key;
  }
  return (arr[1] + arr[2] + arr[3]) / 3.0f;
}

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

// ================== Auto-zero & AutoCal Energi ==================
const float ZERO_I_MAX_A = 0.10f;
const uint32_t ZERO_WINDOW_MS = 6000;
const float I_OFF_MIN = -0.20f;
const float I_OFF_MAX =  0.00f;
uint32_t zeroStartMs = 0; bool zeroWindowActive = false;

static inline bool allRelaysOff(){
  for(int i=0;i<4;i++) if (lastRelayState[i]) return false;
  return true;
}
void autoZeroCurrentIfIdle(float i_inst) {
  if (allRelaysOff() && !isnan(i_inst) && fabsf(i_inst) <= ZERO_I_MAX_A) {
    if (!zeroWindowActive) {
      zeroWindowActive = true;
      zeroStartMs = millis();
    }
    if (zeroWindowActive && (millis() - zeroStartMs >= ZERO_WINDOW_MS)) {
      float new_i_off = constrain(-i_inst, I_OFF_MIN, I_OFF_MAX);
      if (fabsf(new_i_off - calib.i_off) > 0.002f) {
        calib.i_off = new_i_off;
        saveCalib();
        Serial.printf("🤖 Auto-zero: set i_off = %.3f A (idle=%.3f A)\n", calib.i_off, i_inst);
      }
      zeroWindowActive = false;
    }
  } else {
    zeroWindowActive = false;
  }
}

// --- Referensi meter PLN (dua titik) ---
struct MeterRef { double kwh = NAN; uint32_t ts = 0; };
MeterRef refA, refB; double snapA_e = NAN, snapB_e = NAN;

void loadMeterRefs(){
  prefs.begin("meter-ref", true);
  refA.kwh = prefs.getDouble("A_kwh", NAN);
  refA.ts  = prefs.getUInt("A_ts", 0);
  refB.kwh = prefs.getDouble("B_kwh", NAN);
  refB.ts  = prefs.getUInt("B_ts", 0);
  snapA_e  = prefs.getDouble("A_e", NAN);
  snapB_e  = prefs.getDouble("B_e", NAN);
  prefs.end();
}
void saveMeterRefs(){
  prefs.begin("meter-ref", false);
  prefs.putDouble("A_kwh", refA.kwh);
  prefs.putUInt("A_ts", refA.ts);
  prefs.putDouble("B_kwh", refB.kwh);
  prefs.putUInt("B_ts", refB.ts);
  prefs.putDouble("A_e", snapA_e);
  prefs.putDouble("B_e", snapB_e);
  prefs.end();
}
void tryApplyEnergyGainFromRefs(){
  if (isnan(refA.kwh) || isnan(refB.kwh) || isnan(snapA_e) || isnan(snapB_e)) return;
  if (refA.ts <= refB.ts) return;

  const uint32_t MIN_CALIB_SECONDS = 3600 * 6;
  if (refA.ts - refB.ts < MIN_CALIB_SECONDS) {
    Serial.println("⚠ Auto-Calib: Jeda waktu antar referensi terlalu singkat (<6 jam), kalibrasi diabaikan.");
    return;
  }

  double dRef  = refA.kwh  - refB.kwh;
  double dPzem = snapA_e   - snapB_e;

  const double MIN_ENERGY_DELTA_KWH = 0.5;
  if (dRef <= MIN_ENERGY_DELTA_KWH || dPzem <= MIN_ENERGY_DELTA_KWH) {
    Serial.println("⚠ Auto-Calib: Delta energi terlalu kecil (<0.5 kWh), kalibrasi diabaikan.");
    return;
  }
  if (dPzem < 0) {
    Serial.println("⚠ PZEM energy snapshot mundur — abaikan kalibrasi.");
    return;
  }

  double gain = dRef / dPzem;
  if (gain < 0.90) gain = 0.90;
  if (gain > 1.10) gain = 1.10;

  calib.p_gain   *= (float)gain;
  calib.kwh_gain *= (float)gain;
  saveCalib();
  Serial.printf("🤖 Auto-tune energi: dRef=%.3f kWh dPzem=%.3f kWh -> gain=%.4f\n", dRef, dPzem, gain);
  Serial.printf("    p_gain=%.4f  kwh_gain=%.4f (tersimpan)\n", calib.p_gain, calib.kwh_gain);

  refB.kwh = NAN;
  snapB_e  = NAN;
  saveMeterRefs();
}
void handleMeterPush(double kwhRef, uint32_t tsRef){
  if (isnan(kwhRef) || tsRef == 0) return;
  refB = refA;
  snapB_e = snapA_e;
  refA.kwh = kwhRef;
  refA.ts  = tsRef;
  snapA_e  = pzem.energy();
  saveMeterRefs();
  Serial.printf("📥 MeterRef: kWh=%.3f ts=%u | snapshot E=%.5f kWh\n",
      refA.kwh, refA.ts, snapA_e);
  tryApplyEnergyGainFromRefs();
}

// ================== I2C GUARD & ENV SENSORS ==================
struct I2CGuard {
  bool scanAddr(uint8_t addr){
    Wire.beginTransmission(addr);
    return (Wire.endTransmission()==0);
  }
  void busRecover(){
    pinMode(SDA_PIN, INPUT_PULLUP);
    pinMode(SCL_PIN, INPUT_PULLUP);
    for(int i=0;i<9;i++){
      pinMode(SCL_PIN, OUTPUT); delayMicroseconds(5);
      digitalWrite(SCL_PIN, LOW); delayMicroseconds(5);
      pinMode(SCL_PIN, INPUT_PULLUP); delayMicroseconds(5);
    }
    pinMode(SDA_PIN, OUTPUT); digitalWrite(SDA_PIN, HIGH); delayMicroseconds(5);
    pinMode(SCL_PIN, OUTPUT); digitalWrite(SCL_PIN, HIGH); delayMicroseconds(5);
    Wire.begin(SDA_PIN, SCL_PIN);
    Wire.setClock(100000);
    Wire.setTimeOut(50);
  }
} i2cguard;

struct EnvSensors {
  bool bmeOK=false, bhOK=false;
  uint8_t bmeAddr=0x76;
  unsigned failCnt=0;
  unsigned long lastGoodMs=0, lastInitMs=0;
  const unsigned maxFail=8;
  const unsigned long reinitEveryMs=600000;
  const unsigned long staleMs=120000;

  bool begin(){
    lastInitMs=millis();
    if(!i2cguard.scanAddr(bmeAddr)){
      Serial.println("I2C: BME280 tak terdeteksi, recovery bus...");
      i2cguard.busRecover();
    }
    bmeOK = bme.begin(bmeAddr);
    if(!bmeOK){
      bmeAddr=0x77;
      if(!i2cguard.scanAddr(bmeAddr)) i2cguard.busRecover();
      bmeOK = bme.begin(bmeAddr);
    }
    bhOK  = lightMeter.begin(BH1750::CONTINUOUS_HIGH_RES_MODE);
    if(!bhOK){
      lightMeter.configure(BH1750::CONTINUOUS_HIGH_RES_MODE);
      bhOK = i2cguard.scanAddr(0x23) || i2cguard.scanAddr(0x5C);
    }
    Serial.printf("BME280 : %s (addr 0x%02X)\n", bmeOK?"OK":"FAIL", bmeAddr);
    Serial.printf("BH1750 : %s\n", bhOK?"OK":"FAIL");
    failCnt=0;
    return (bmeOK||bhOK);
  }
  void autoRecover(){
    unsigned long now=millis();
    if (failCnt>=maxFail){
      Serial.println("I2C: gagal beruntun — RECOVER bus & reinit sensors");
      i2cguard.busRecover(); begin();
    } else if ((now-lastInitMs)>reinitEveryMs){
      Serial.println("I2C: reinit berkala (prevent lock-up)");
      begin();
    } else if (lastGoodMs>0 && (now-lastGoodMs)>staleMs){
      Serial.println("I2C: bacaan stale — reinit");
      begin();
    }
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
    if (ok && any){
      failCnt=0; lastGoodMs=millis();
    }
    if (!ok) autoRecover();
    return any;
  }
} env;

// ================== SUPABASE: Helper umum ==================
void addSupabaseAuthHeaders(HTTPClient &https){
  https.addHeader("apikey", SUPABASE_API_KEY);
  https.addHeader("Authorization", String("Bearer ") + SUPABASE_API_KEY);
}

// ================== SUPABASE: Kirim riwayat monitoring_log ==================
bool sendToSupabaseLog(
  const String& tsISO,
  float suhu, float kelembapan, float tekanan,
  float altitude, float lightLevel
){
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("⚠ Supabase Log: WiFi tidak terhubung, skip.");
    return false;
  }

  int nonNan = 0;
  if(!isnan(filt.v)) nonNan++;
  if(!isnan(filt.i)) nonNan++;
  if(!isnan(filt.p)) nonNan++;
  if(!isnan(filt.S)) nonNan++;
  if(!isnan(filt.Q)) nonNan++;
  if(!isnan(filt.pf)) nonNan++;
  if(!isnan(filt.f)) nonNan++;
  if(!isnan(suhu)) nonNan++;
  if(!isnan(kelembapan)) nonNan++;
  if(!isnan(tekanan)) nonNan++;
  if(!isnan(altitude)) nonNan++;
  if(!isnan(lightLevel)) nonNan++;
  if(nonNan == 0) {
    Serial.println("⚠ Supabase Log: semua nilai NaN, tidak dikirim.");
    return false;
  }

  WiFiClientSecure client;
  client.setInsecure();

  HTTPClient https;
  String url = String(SUPABASE_URL) + "/rest/v1/" + TABLE_LOG;

  if (!https.begin(client, url)) {
    Serial.println("❌ Supabase Log: gagal begin() HTTPClient.");
    return false;
  }

  addSupabaseAuthHeaders(https);
  https.addHeader("Content-Type", "application/json");
  https.addHeader("Prefer", "return=minimal");

  String payload = "{";
  payload += "\"device_id\":\"" + String(DEVICE_ID) + "\"";
  payload += ",\"ts\":\"" + tsISO + "\"";

  if(!isnan(filt.v))  payload += ",\"tegangan_v\":"        + String(filt.v,3);
  if(!isnan(filt.i))  payload += ",\"arus_a\":"            + String(filt.i,4);
  if(!isnan(filt.p))  payload += ",\"daya_aktif_w\":"      + String(filt.p,3);
  if(!isnan(filt.S))  payload += ",\"daya_semu_va\":"      + String(filt.S,3);
  if(!isnan(filt.Q))  payload += ",\"daya_reaktif_var\":"  + String(filt.Q,3);
  if(!isnan(filt.f))  payload += ",\"frekuensi_hz\":"      + String(filt.f,3);
  if(!isnan(filt.pf)) payload += ",\"faktor_daya\":"       + String(filt.pf,4);

  float E_total = pzem.energy();
  if(!isnan(E_total))   payload += ",\"energi_total_kwh\":"   + String(E_total,5);
  payload += ",\"energi_harian_kwh\":"  + String(today_kwh,5);
  payload += ",\"energi_bulanan_kwh\":" + String(month_kwh,5);

  if(!isnan(suhu))       payload += ",\"suhu_c\":"         + String(suhu,2);
  if(!isnan(kelembapan)) payload += ",\"kelembapan_rh\":"  + String(kelembapan,2);
  if(!isnan(tekanan))    payload += ",\"tekanan_hpa\":"    + String(tekanan,2);
  if(!isnan(altitude))   payload += ",\"altitude_m\":"     + String(altitude,2);
  if(!isnan(lightLevel)) payload += ",\"light_level_lux\":"+ String(lightLevel,2);

  payload += ",\"wifi_rssi\":" + String(WiFi.RSSI());
  payload += "}";

  Serial.println("➡ Supabase Log payload:");
  Serial.println(payload);

  int httpCode = https.POST(payload);
  bool ok = false;

  if (httpCode > 0) {
    Serial.printf("📡 Supabase Log HTTP status: %d\n", httpCode);
    if (httpCode == 201 || httpCode == 200 || httpCode == 204) {
      ok = true;
    } else {
      String resp = https.getString();
      Serial.print("Respon Supabase Log: ");
      Serial.println(resp);
    }
  } else {
    Serial.printf("❌ Supabase Log POST gagal, error: %s\n",
                  https.errorToString(httpCode).c_str());
  }

  https.end();
  if (ok) Serial.println("✅ Supabase: monitoring_log tersimpan.");
  else    Serial.println("❌ Supabase: gagal simpan monitoring_log.");
  return ok;
}

// ================== SUPABASE: Relay ==================
struct RelayConfig {
  bool valid = false;
  bool state = false;
  String waktuOn;
  String waktuOff;
  bool day[7]; // 0=Sun..6=Sat
};
RelayConfig relayCfg[4];
unsigned long lastRelayFetchMs = 0;
const unsigned long RELAY_FETCH_INTERVAL_MS = 10000;

bool upsertRelayStateSupabase(int ch, bool on, const char* byTag){
  if (WiFi.status() != WL_CONNECTED) return false;
  WiFiClientSecure client; client.setInsecure();
  HTTPClient https;
  String url = String(SUPABASE_URL) + "/rest/v1/" + TABLE_RELAY +
               "?on_conflict=device_id,channel";

  if (!https.begin(client, url)){
    Serial.println("❌ Supabase Relay: gagal begin() upsert.");
    return false;
  }

  addSupabaseAuthHeaders(https);
  https.addHeader("Content-Type", "application/json");
  https.addHeader("Prefer", "resolution=merge-duplicates,return=minimal");

  String payload = "[{";
  payload += "\"device_id\":\"" + String(DEVICE_ID) + "\"";
  payload += ",\"channel\":" + String(ch);
  payload += ",\"state\":" + String(on ? "true":"false");
  payload += ",\"meta_by\":\"" + String(byTag) + "\"";
  payload += ",\"meta_ts\":\"" + isoTimeWIB() + "\"";
  payload += "}]";

  int code = https.POST(payload);
  bool ok = false;
  if (code > 0){
    Serial.printf("📡 Supabase Relay upsert HTTP: %d\n", code);
    if (code==201 || code==200 || code==204) ok = true;
    else {
      String resp = https.getString();
      Serial.print("Respon Relay upsert: "); Serial.println(resp);
    }
  } else {
    Serial.printf("❌ Relay upsert error: %s\n", https.errorToString(code).c_str());
  }
  https.end();
  return ok;
}

void scheduleSetRelay(int idx, bool on, const char* byTag){
  applyRelay(idx,on);
  upsertRelayStateSupabase(idx,on,byTag);
}

bool initRelayDefaultsInSupabase(){
  if (WiFi.status() != WL_CONNECTED) return false;
  WiFiClientSecure client; client.setInsecure();
  HTTPClient https;
  String url = String(SUPABASE_URL) + "/rest/v1/" + TABLE_RELAY +
               "?on_conflict=device_id,channel";

  if (!https.begin(client, url)){
    Serial.println("❌ Supabase Relay init: gagal begin().");
    return false;
  }
  addSupabaseAuthHeaders(https);
  https.addHeader("Content-Type", "application/json");
  https.addHeader("Prefer", "resolution=merge-duplicates,return=minimal");

  String payload = "[";
  for (int ch=0; ch<4; ++ch){
    if (ch>0) payload += ",";
    payload += "{";
    payload += "\"device_id\":\"" + String(DEVICE_ID) + "\"";
    payload += ",\"channel\":" + String(ch);
    payload += ",\"state\":false";
    payload += ",\"sun\":true,\"mon\":true,\"tue\":true,\"wed\":true,\"thu\":true,\"fri\":true,\"sat\":true";
    payload += "}";
  }
  payload += "]";

  int code = https.POST(payload);
  bool ok = false;
  if (code>0){
    Serial.printf("📡 Relay init HTTP: %d\n", code);
    if (code==201 || code==200 || code==204) ok=true;
    else {
      String resp = https.getString();
      Serial.print("Respon Relay init: "); Serial.println(resp);
    }
  } else {
    Serial.printf("❌ Relay init error: %s\n", https.errorToString(code).c_str());
  }
  https.end();
  return ok;
}

void fetchRelayConfigFromSupabase(){
  if (WiFi.status() != WL_CONNECTED) return;
  if (millis() - lastRelayFetchMs < RELAY_FETCH_INTERVAL_MS) return;
  lastRelayFetchMs = millis();

  WiFiClientSecure client; client.setInsecure();
  HTTPClient https;
  String url = String(SUPABASE_URL) + "/rest/v1/" + TABLE_RELAY +
               "?device_id=eq." + DEVICE_ID + "&select=*";

  if (!https.begin(client, url)){
    Serial.println("❌ Relay fetch: gagal begin().");
    return;
  }
  addSupabaseAuthHeaders(https);
  https.addHeader("Accept", "application/json");

  int code = https.GET();
  if (code <= 0){
    Serial.printf("❌ Relay fetch error: %s\n", https.errorToString(code).c_str());
    https.end();
    return;
  }
  if (code != 200){
    Serial.printf("⚠ Relay fetch HTTP: %d\n", code);
    String resp = https.getString();
    Serial.println(resp);
    https.end();
    return;
  }

  String resp = https.getString();
  https.end();

  DynamicJsonDocument doc(4096);
  auto err = deserializeJson(doc, resp);
  if (err){
    Serial.print("❌ JSON Relay parse: "); Serial.println(err.c_str());
    return;
  }
  if (!doc.is<JsonArray>()) {
    Serial.println("⚠ Relay fetch: respon bukan array.");
    return;
  }

  // reset valid
  for(int i=0;i<4;i++) relayCfg[i].valid = false;

  JsonArray arr = doc.as<JsonArray>();
  if (arr.size()==0){
    Serial.println("⚠ relay_channel kosong, inisialisasi default...");
    initRelayDefaultsInSupabase();
    return;
  }

  for(JsonObject row : arr){
    int ch = row["channel"] | -1;
    if (ch<0 || ch>3) continue;

    RelayConfig &cfg = relayCfg[ch];
    cfg.valid = true;

    bool dbState = row["state"] | false;
    cfg.state = dbState;

    // apply remote state -> hardware
    if (dbState != lastRelayState[ch]){
      Serial.printf("[RELAY-REMOTE] ch=%d -> %s (Supabase)\n", ch, dbState?"ON":"OFF");
      applyRelay(ch, dbState);
    }

    const char* won  = row["waktu_on"].isNull()  ? nullptr : row["waktu_on"].as<const char*>();
    const char* woff = row["waktu_off"].isNull() ? nullptr : row["waktu_off"].as<const char*>();

    cfg.waktuOn  = won  ? String(won).substring(0,5)  : "";
    cfg.waktuOff = woff ? String(woff).substring(0,5) : "";

    cfg.day[0] = row["sun"].isNull() ? true : row["sun"].as<bool>();
    cfg.day[1] = row["mon"].isNull() ? true : row["mon"].as<bool>();
    cfg.day[2] = row["tue"].isNull() ? true : row["tue"].as<bool>();
    cfg.day[3] = row["wed"].isNull() ? true : row["wed"].as<bool>();
    cfg.day[4] = row["thu"].isNull() ? true : row["thu"].as<bool>();
    cfg.day[5] = row["fri"].isNull() ? true : row["fri"].as<bool>();
    cfg.day[6] = row["sat"].isNull() ? true : row["sat"].as<bool>();
  }
}

// ================== SUPABASE: Device Commands ==================
unsigned long lastCmdPollMs = 0;
const unsigned long CMD_POLL_INTERVAL_MS = 5000;

bool markCommandProcessed(long id){
  if (WiFi.status() != WL_CONNECTED) return false;
  WiFiClientSecure client; client.setInsecure();
  HTTPClient https;
  String url = String(SUPABASE_URL) + "/rest/v1/" + TABLE_CMD +
               "?id=eq." + String(id);

  if (!https.begin(client, url)){
    Serial.println("❌ markCommandProcessed: gagal begin().");
    return false;
  }
  addSupabaseAuthHeaders(https);
  https.addHeader("Content-Type", "application/json");
  https.addHeader("Prefer", "return=minimal");

  String payload = "{\"processed\":true,\"processed_at\":\"" + isoTimeWIB() + "\"}";
  int code = https.PATCH(payload);
  bool ok=false;
  if (code>0){
    if (code==200 || code==204) ok=true;
    else {
      String resp = https.getString();
      Serial.printf("⚠ mark processed HTTP %d: %s\n", code, resp.c_str());
    }
  } else {
    Serial.printf("❌ mark processed error: %s\n", https.errorToString(code).c_str());
  }
  https.end();
  return ok;
}

void handleCalibrateCommand(JsonObject cmd){
  double v_gain   = cmd["v_gain"]   | NAN;
  double v_off    = cmd["v_off"]    | NAN;
  double i_gain   = cmd["i_gain"]   | NAN;
  double i_off    = cmd["i_off"]    | NAN;
  double p_gain   = cmd["p_gain"]   | NAN;
  double p_off    = cmd["p_off"]    | NAN;
  double kwh_gain = cmd["kwh_gain"] | NAN;

  if (!isnan(v_gain))   calib.v_gain   = (float)v_gain;
  if (!isnan(v_off))    calib.v_off    = (float)v_off;
  if (!isnan(i_gain))   calib.i_gain   = (float)i_gain;
  if (!isnan(i_off))    calib.i_off    = (float)i_off;
  if (!isnan(p_gain))   calib.p_gain   = (float)p_gain;
  if (!isnan(p_off))    calib.p_off    = (float)p_off;
  if (!isnan(kwh_gain)) calib.kwh_gain = (float)kwh_gain;

  saveCalib();
  Serial.printf("🔧 Calib updated: V(%.4f, %.3f) I(%.4f, %.3f) P(%.4f, %.3f) kWhGain=%.4f\n",
    calib.v_gain,calib.v_off,
    calib.i_gain,calib.i_off,
    calib.p_gain,calib.p_off,
    calib.kwh_gain);
}

void handleViRefCommand(JsonObject cmd){
  double v_ref = cmd["v_ref"] | NAN;
  double i_ref = cmd["i_ref"] | NAN;

  PZEMRead r = robustReadPZEM5(60);

  if (!isnan(v_ref) && v_ref>80 && v_ref<280 && !isnan(r.v) && r.v>50){
    float new_gain = (float)(v_ref / r.v);
    calib.v_gain = constrain(new_gain, 0.90f, 1.10f);
    Serial.printf("🔧 Calib V: raw=%.2f ref=%.2f -> v_gain=%.4f\n", r.v, v_ref, calib.v_gain);
  }
  if (!isnan(i_ref) && i_ref>=0 && i_ref<100 && !isnan(r.i) && r.i>=0){
    float new_gain = (r.i>0.01f)? (float)(i_ref / r.i) : calib.i_gain;
    calib.i_gain = constrain(new_gain, 0.90f, 1.20f);
    Serial.printf("🔧 Calib I: raw=%.3f ref=%.3f -> i_gain=%.4f\n", r.i, i_ref, calib.i_gain);
  }
  saveCalib();
}

void pollDeviceCommandsSupabase(){
  if (WiFi.status() != WL_CONNECTED) return;
  if (millis() - lastCmdPollMs < CMD_POLL_INTERVAL_MS) return;
  lastCmdPollMs = millis();

  WiFiClientSecure client; client.setInsecure();
  HTTPClient https;
  String url = String(SUPABASE_URL) + "/rest/v1/" + TABLE_CMD
               + "?device_id=eq." + DEVICE_ID
               + "&processed=is.false&order=created_at.asc&limit=8";

  if (!https.begin(client, url)){
    Serial.println("❌ Commands fetch: gagal begin().");
    return;
  }
  addSupabaseAuthHeaders(https);
  https.addHeader("Accept", "application/json");

  int code = https.GET();
  if (code <= 0){
    Serial.printf("❌ Commands fetch error: %s\n", https.errorToString(code).c_str());
    https.end();
    return;
  }
  if (code != 200){
    Serial.printf("⚠ Commands fetch HTTP: %d\n", code);
    String r = https.getString();
    Serial.println(r);
    https.end();
    return;
  }

  String resp = https.getString();
  https.end();

  DynamicJsonDocument doc(4096);
  auto err = deserializeJson(doc, resp);
  if (err){
    Serial.print("❌ JSON Commands parse: "); Serial.println(err.c_str());
    return;
  }
  if (!doc.is<JsonArray>()) return;

  JsonArray arr = doc.as<JsonArray>();
  if (arr.size()==0) return;

  Serial.printf("📥 %d perintah dari Supabase.\n", arr.size());

  for (JsonObject cmd : arr){
    long id = cmd["id"] | 0;
    const char* type = cmd["cmd_type"] | "";
    if (id<=0 || type[0]=='\0') continue;

    Serial.printf("➡ Handle command #%ld type=%s\n", id, type);

    if (strcmp(type,"reset_kwh")==0){
      resetEnergyCountersLocal();
    } else if (strcmp(type,"calibrate")==0){
      handleCalibrateCommand(cmd);
    } else if (strcmp(type,"vi_ref")==0){
      handleViRefCommand(cmd);
    } else if (strcmp(type,"meter_push")==0){
      double kwhRef = cmd["meter_kwh_ref"] | NAN;
      long tsRef    = cmd["meter_ts"]      | 0;
      handleMeterPush(kwhRef, (uint32_t)tsRef);
    }

    markCommandProcessed(id);
  }
}

// ================== SETUP ==================
void setup(){
  Serial.begin(115200);
  delay(300);
  setCpuFrequencyMhz(80);
  Serial.printf("⚙  CPU set ke %d MHz\n", getCpuFrequencyMhz());

  for(int i=0;i<4;i++){
    pinMode(relayPins[i], OUTPUT);
    digitalWrite(relayPins[i], HIGH); // OFF
  }
  pinMode(BUZZER_PIN, OUTPUT);
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(BUZZER_PIN, LOW);
  digitalWrite(LED_PIN, LOW);
  analogReadResolution(12);

  Wire.begin(SDA_PIN, SCL_PIN);
  Wire.setClock(100000);
  Wire.setTimeOut(50);

  waitForWifi();
  ensureTime();
  configTime(TZ_OFFSET, DST_OFFSET, "pool.ntp.org","time.google.com");

  Serial.println("\n🔎 Inisialisasi sensor lingkungan (dengan guard)...");
  env.begin();

  Serial.println("\n=== PZEM Monitor (ESP32-S3 Mini) — Detail ===");
  Serial.printf("UART1: RX=%d  TX=%d\n", PZEM_RX_PIN, PZEM_TX_PIN);
  PZEMSerial.begin(9600, SERIAL_8N1, PZEM_RX_PIN, PZEM_TX_PIN);

  loadMonthBaseline();
  loadCalib();
  loadMeterRefs();
  Serial.printf("🔧 Calib awal: V(%.4f, %.3f) I(%.4f, %.3f) P(%.4f, %.3f) kWhGain=%.4f\n",
    calib.v_gain,calib.v_off,
    calib.i_gain,calib.i_off,
    calib.p_gain,calib.p_off,
    calib.kwh_gain);

  // Relay: pastikan ada row di Supabase, lalu fetch config awal
  initRelayDefaultsInSupabase();
  fetchRelayConfigFromSupabase();

  Serial.println("\n======================================================");
  Serial.println(" ESP32-S3 + PZEM + BME280/BH1750 + Relay + Scheduler + Supabase");
  Serial.println("======================================================\n");
}

// ================== LOOP ==================
void loop(){
  if (wifiMulti.run() != WL_CONNECTED){
    Serial.println("📡 WiFi putus. Reconnect...");
    rguard.markWifiReconn();
  }

  // Poll konfigurasi relay & perintah dari Supabase
  fetchRelayConfigFromSupabase();
  pollDeviceCommandsSupabase();

  // ======= Scheduler Relay =======
  unsigned long nowMs = millis();
  if(nowMs - lastScheduleTick >= TICK_MS){
    lastScheduleTick = nowMs;
    String hhmm = now_HHMM();
    if(hhmm.length() && hhmm != lastMinuteChecked){
      lastMinuteChecked = hhmm;
      int nowMin = nowMinutes();
      int dIdx   = dayIndex(); // 0..6

      for(int i=0;i<4;i++){
        RelayConfig &cfg = relayCfg[i];
        if (!cfg.valid) continue;

        bool todayEnabled = cfg.day[dIdx];
        if(!todayEnabled) continue;

        bool haveOn  = (cfg.waktuOn.length()  >= 5);
        bool haveOff = (cfg.waktuOff.length() >= 5);

        if (haveOn && haveOff){
          bool shouldBeOn = isInWindow(cfg.waktuOn, cfg.waktuOff, nowMin);
          if(shouldBeOn && !lastRelayState[i]){
            scheduleSetRelay(i, true, "schedule_hold");
          } else if(!shouldBeOn && lastRelayState[i]){
            scheduleSetRelay(i, false, "schedule_hold");
          }
        } else {
          if(haveOn && cfg.waktuOn.substring(0,5)==hhmm && !lastRelayState[i]){
            scheduleSetRelay(i, true, "schedule");
          }
          if(haveOff && cfg.waktuOff.substring(0,5)==hhmm && lastRelayState[i]){
            scheduleSetRelay(i, false, "schedule");
          }
        }
        delay(10);
      }
    }
  }

  // ======= PZEM + energi =======
  {
    PZEMRead r = robustReadPZEM5(80);
    if (r.ok){
      float V  = r.v  * calib.v_gain + calib.v_off;
      float I  = r.i  * calib.i_gain + calib.i_off;
      float Pm = r.p  * calib.p_gain + calib.p_off;
      float E  = r.e;
      float F  = r.f;
      float PF = constrain(r.pf, 0.0f, 1.0f);

      float Pcalc = V * I * PF;
      float mismatch = (Pm>1.0f) ? fabsf(Pm - Pcalc)/Pm : 0.0f;
      mismatch = constrain(mismatch, 0.0f, 1.0f);
      float w_sensor = 1.0f - constrain((mismatch - 0.05f)/0.30f, 0.0f, 1.0f);
      float P = w_sensor*Pm + (1.0f - w_sensor)*Pcalc;

      float S = V * I;
      float Q = NAN;
      if (PF > 0 && PF <= 1.0f){
        float S2=S*S, P2=P*P;
        Q = (S2>P2)? sqrtf(S2-P2):0.0f;
      }

      filt.v=ema(filt.v,V);
      filt.i=ema(filt.i,I);
      filt.p=ema(filt.p,P);
      filt.e=ema(filt.e,E);
      filt.f=ema(filt.f,F);
      filt.pf=ema(filt.pf,PF);
      filt.S=ema(filt.S,S);
      filt.Q=ema(filt.Q,Q);

      autoZeroCurrentIfIdle(filt.i);

      time_t nowt=time(nullptr); struct tm tm_info;
      localtime_r(&nowt,&tm_info);

      auto apply_delta = [&](float delta){
        if (delta>0 && delta < 0.50f){
          float delta_corr = delta * calib.kwh_gain;
          today_kwh += delta_corr;
          month_kwh += delta_corr;
          lastE_kwh = E;
        }
      };

      if (timeReady()){
        int y=tm_info.tm_year+1900;
        int m=tm_info.tm_mon+1;
        int d=tm_info.tm_mday;
        if (lastDay==-1){
          lastYear=y; lastMonth=m; lastDay=d; lastE_kwh=E;
        }
        if (m!=lastMonth || y!=lastYear){
          saveMonthBaseline(month_kwh, lastYear, lastMonth);
          month_kwh=0.0f;
          lastMonth=m; lastYear=y; lastE_kwh=E;
        }
        if (d!=lastDay){
          today_kwh=0.0f;
          lastDay=d; lastE_kwh=E;
        }
        float delta = E - lastE_kwh;
        if (delta < 0){
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
        Serial.printf("\n[Ringkasan Energi] Harian: %0.3f kWh | Bulanan: %0.3f kWh | gain(kWh)=%.4f\n",
                      today_kwh, month_kwh, calib.kwh_gain);
      }
    } else {
      if (millis()-lastPrint > PRINT_EVERY_MS){
        lastPrint = millis();
        Serial.println("NaN: cek TX/RX silang, GND, 5V, dan pastikan CT hanya lewat SATU konduktor (L).");
      }
    }
  }

  // ======= Sensor Lingkungan =======
  float suhu=NAN, kelembapan=NAN, tekanan=NAN, altitude=NAN, lightLevel=NAN;
  env.read(suhu, kelembapan, tekanan, altitude, lightLevel);

  // ======= Kirim ke Supabase (riwayat) =======
  unsigned long nowMs2 = millis();
  if (nowMs2 - lastSendMs >= SEND_INTERVAL_MS){
    lastSendMs = nowMs2;
    String tsISO = isoTimeWIB();

    Serial.println("------------------------------------------------------");
    Serial.printf("⏱  %s\n", tsISO.c_str());
    Serial.printf("Device ID   : %s\n", DEVICE_ID);
    Serial.printf("WiFi RSSI   : %d dBm\n", WiFi.RSSI());
    Serial.printf("Buzzer      : %s\n", (digitalRead(BUZZER_PIN)==HIGH) ? "ON" : "OFF");

    bool okLog = sendToSupabaseLog(tsISO, suhu, kelembapan, tekanan, altitude, lightLevel);

    if (okLog){
      digitalWrite(LED_PIN, HIGH);
      delay(1000);          // LED nyala 1 detik
      digitalWrite(LED_PIN, LOW);
    }
  }

  delay(5);
}
