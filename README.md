# sensor-pzem-004t-v4
menggunkan firebase

insert into public.device_commands (device_id, cmd_type)
values ('ESP32-S3-Monitoring-01', 'reset_kwh');



### Tabel & policy monitoring_log:

        -- 1) Tabel log monitoring
        create table if not exists public.monitoring_log (
          id           bigint generated always as identity primary key,
          device_id    text not null,
          ts           timestamptz not null default now(),
        
          -- Data listrik
          tegangan_v           double precision,
          arus_a               double precision,
          daya_aktif_w         double precision,
          daya_semu_va         double precision,
          daya_reaktif_var     double precision,
          frekuensi_hz         double precision,
          faktor_daya          double precision,
          energi_total_kwh     double precision,
          energi_harian_kwh    double precision,
          energi_bulanan_kwh   double precision,
        
          -- Data lingkungan
          suhu_c               double precision,
          kelembapan_rh        double precision,
          tekanan_hpa          double precision,
          altitude_m           double precision,
          light_level_lux      double precision,
        
          -- Info tambahan
          wifi_rssi            integer
        );
        
        -- 2) Aktifkan RLS
        alter table public.monitoring_log enable row level security;
        
        -- 3) Policy: izinkan insert dari anon key
        create policy "monitoring_log_anon_insert"
        on public.monitoring_log
      for insert
      to public
      with check (true);


      

### Tabel & policy relay_channel


                        -- 1) Tabel relay
                        create table if not exists public.relay_channel (
                          id        bigint generated always as identity primary key,
                          device_id text not null,
                          channel   smallint not null check (channel between 0 and 3),
                        
                          state     boolean not null default false,
                          meta_by   text,
                          meta_ts   timestamptz,
                        
                          -- Jadwal (opsional)
                          waktu_on  time,
                          waktu_off time,
                        
                          -- Hari aktif
                          sun boolean default true,
                          mon boolean default true,
                          tue boolean default true,
                          wed boolean default true,
                          thu boolean default true,
                          fri boolean default true,
                          sat boolean default true,
                        
                          unique (device_id, channel)
                        );
                        
                        -- 2) Aktifkan RLS
                        alter table public.relay_channel enable row level security;
                        
                        -- 3) Policy: boleh SELECT (dibaca dari dashboard / device)
                        create policy "relay_channel_anon_select"
                        on public.relay_channel
                        for select
                        to public
                        using (true);
                        
                        -- 4) Policy: boleh INSERT (buat row baru)
                        create policy "relay_channel_anon_insert"
                        on public.relay_channel
                        for insert
                        to public
                        with check (true);
                        
                        -- 5) Policy: boleh UPDATE (ubah state, jadwal, dll)
                        create policy "relay_channel_anon_update"
                        on public.relay_channel
                        for update
                        to public
                        using (true)
                        with check (true);


                        
                        
### Tabel & policy device_commands :

            -- 1) Tabel perintah ke device
            create table if not exists public.device_commands (
              id       bigint generated always as identity primary key,
              device_id text not null,
              cmd_type  text not null,  -- 'reset_kwh','calibrate','vi_ref','meter_push'
            
              -- Untuk cmd_type = 'calibrate'
              v_gain   double precision,
              v_off    double precision,
              i_gain   double precision,
              i_off    double precision,
              p_gain   double precision,
              p_off    double precision,
              kwh_gain double precision,
            
              -- Untuk cmd_type = 'vi_ref'
              v_ref    double precision,
              i_ref    double precision,
            
              -- Untuk cmd_type = 'meter_push'
              meter_kwh_ref double precision,
              meter_ts      bigint,   -- epoch detik
            
              processed    boolean not null default false,
              created_at   timestamptz not null default now(),
              processed_at timestamptz
            );
            
            -- 2) Aktifkan RLS
            alter table public.device_commands enable row level security;
            
            -- 3) Policy: boleh INSERT (web/app kirim perintah)
            create policy "device_commands_anon_insert"
            on public.device_commands
            for insert
            to public
            with check (true);
            
            -- 4) Policy: boleh SELECT (device ESP baca perintah)
            create policy "device_commands_anon_select"
            on public.device_commands
            for select
            to public
            using (true);
            
            -- 5) Policy: boleh UPDATE (device ESP tandaikan processed=true)
            create policy "device_commands_anon_update"
            on public.device_commands
            for update
            to public
            using (true)
            with check (true);

 



      
