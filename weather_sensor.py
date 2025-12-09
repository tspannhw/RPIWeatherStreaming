#!/usr/bin/env python3
"""
Weather Sensor Data Reader for Raspberry Pi with Pimoroni Weather HAT

This module reads data from the Pimoroni Weather HAT:
- BME280: Temperature, Pressure, Humidity
- LTR-559: Light (lux) and proximity sensor
- System metrics: CPU temp, memory, disk usage

Based on: https://shop.pimoroni.com/products/weather-hat-only

PERFORMANCE: Uses asynchronous sensor reading architecture
- Background thread updates sensors every 5 seconds
- Main thread returns cached values instantly (< 1ms)
- No blocking on slow sensor reads!
"""

import uuid
import time
import socket
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, Optional
import json

logger = logging.getLogger(__name__)

# Try to import Weather HAT library
WEATHER_HAT_AVAILABLE = False
try:
    import weatherhat
    WEATHER_HAT_AVAILABLE = True
    logger.info("Weather HAT library imported successfully")
except ImportError as e:
    logger.warning(f"Weather HAT library not available - using simulation mode: {e}")


class WeatherSensor:
    """Read weather sensor data from Pimoroni Weather HAT."""
    
    def __init__(self, simulate: bool = False, require_real_sensors: bool = False):
        """
        Initialize weather sensor reader.
        
        Args:
            simulate: If True, generate simulated data instead of reading real sensors
            require_real_sensors: If True, raise error if physical sensors not available
                                  (PRODUCTION MODE - no fallback to simulation)
        
        Raises:
            RuntimeError: If require_real_sensors=True and sensors unavailable
        """
        # PRODUCTION MODE: Enforce real sensors
        if require_real_sensors:
            if not WEATHER_HAT_AVAILABLE:
                raise RuntimeError(
                    "PRODUCTION MODE FAILED: Weather HAT library not available. "
                    "Install required package: weatherhat"
                )
            if simulate:
                raise RuntimeError(
                    "PRODUCTION MODE FAILED: Simulation mode requested but "
                    "require_real_sensors=True. Cannot use simulated data."
                )
            logger.info("PRODUCTION MODE: Real sensors required and enforced")
            self.simulate = False
        else:
            # Development/test mode: Allow simulation fallback
            self.simulate = simulate or not WEATHER_HAT_AVAILABLE
        
        # Read these ONLY ONCE at startup
        self.hostname = socket.gethostname()
        self.mac_address = self._get_mac_address()
        self.ip_address = self._get_ip_address()
        
        # Cache for system metrics (CPU, memory, disk) - updated once per minute
        self._system_metrics_cache = {
            'cpu_temp': 0.0,
            'cpu_usage': 0.0,
            'memory_usage': 0.0,
            'disk_usage': "0.0 MB",
            'last_update': 0.0
        }
        self._system_metrics_cache_duration = 60.0  # Cache for 60 seconds
        
        # Cache for weather sensors - updated asynchronously
        self._sensor_cache = {
            'temperature': 0.0,
            'humidity': 0.0,
            'pressure': 0.0,
            'device_temperature': 0.0,
            'dewpoint': 0.0,
            'lux': 0.0,
            'last_update': 0.0,
            'update_count': 0
        }
        self._sensor_cache_lock = threading.Lock()
        self._sensor_update_interval = 3.0  # Update sensors every 3 seconds (maybe want 5 seconds or 10)
        self._sensor_thread = None
        self._sensor_thread_running = False
        
        if not self.simulate:
            try:
                self._init_sensors()
                logger.info("Weather HAT initialized successfully")
                if require_real_sensors:
                    self._verify_sensors()
                # Start background sensor reading thread
                self._start_sensor_thread()
            except Exception as e:
                error_msg = f"Failed to initialize Weather HAT: {e}"
                if require_real_sensors:
                    raise RuntimeError(
                        f"PRODUCTION MODE FAILED: {error_msg}. "
                        "Physical sensors required but initialization failed."
                    )
                logger.warning(error_msg)
                logger.info("Falling back to simulation mode")
                self.simulate = True
        else:
            logger.info("Running in simulation mode")
        
        # For simulation - track values for realistic variation
        self.sim_base = {
            'temperature': 77.0,     # Room temperature in Fahrenheit
            'humidity': 15.0,        # Typical indoor humidity %
            'pressure': 1020.0,      # Pressure in hPa
            'lux': 24.0             # Indoor light level
        }
    
    def _init_sensors(self):
        """Initialize Weather HAT."""
        if self.simulate:
            return
        
        try:
            self.weather_hat = weatherhat.WeatherHAT()
            logger.info("Weather HAT sensor initialized")
        except Exception as e:
            self.weather_hat = None
            logger.warning(f"Weather HAT not found: {e}")
            raise
    
    def _verify_sensors(self):
        """Verify Weather HAT is available (for production mode)."""
        if not hasattr(self, 'weather_hat') or self.weather_hat is None:
            raise RuntimeError(
                "PRODUCTION MODE FAILED: No Weather HAT initialized."
            )
        logger.info("[OK] Sensor verification passed - Weather HAT available")
    
    def _get_mac_address(self) -> str:
        """Get MAC address of the primary network interface."""
        try:
            import psutil
            nics = psutil.net_if_addrs()
            # Try wlan0 first (WiFi on Raspberry Pi)
            if 'wlan0' in nics:
                nic = nics['wlan0']
                for i in nic:
                    if i.family == psutil.AF_LINK:
                        return i.address
            # Try eth0 (Ethernet)
            if 'eth0' in nics:
                nic = nics['eth0']
                for i in nic:
                    if i.family == psutil.AF_LINK:
                        return i.address
        except:
            pass
        
        # Fallback
        import uuid
        mac = ':'.join(['{:02x}'.format((uuid.getnode() >> i) & 0xff) 
                       for i in range(0, 8*6, 8)][::-1])
        return mac
    
    def _get_ip_address(self) -> str:
        """Get local IP address."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return "127.0.0.1"
    
    def _update_system_metrics_cache(self):
        """Update cached system metrics (CPU, memory, disk) - called max once per minute."""
        current_time = time.time()
        
        # Check if cache is still valid
        if current_time - self._system_metrics_cache['last_update'] < self._system_metrics_cache_duration:
            return  # Cache is still fresh
        
        # Update CPU temperature
        try:
            with open('/sys/devices/virtual/thermal/thermal_zone0/temp', 'r') as f:
                cputemp = f.readline().strip()
                self._system_metrics_cache['cpu_temp'] = round(float(cputemp) / 1000.0)
        except:
            self._system_metrics_cache['cpu_temp'] = 0.0
        
        # Update CPU usage (non-blocking)
        try:
            import psutil
            # Use interval=None for instant/cached reading (non-blocking)
            self._system_metrics_cache['cpu_usage'] = psutil.cpu_percent(interval=None)
        except:
            self._system_metrics_cache['cpu_usage'] = 0.0
        
        # Update memory usage
        try:
            import psutil
            self._system_metrics_cache['memory_usage'] = psutil.virtual_memory().percent
        except:
            self._system_metrics_cache['memory_usage'] = 0.0
        
        # Update disk usage
        try:
            import psutil
            disk = psutil.disk_usage('/')
            free_mb = disk.free / (1024 * 1024)
            self._system_metrics_cache['disk_usage'] = f"{free_mb:.1f} MB"
        except:
            self._system_metrics_cache['disk_usage'] = "0.0 MB"
        
        # Update timestamp
        self._system_metrics_cache['last_update'] = current_time
        logger.debug(f"System metrics cache updated at {current_time}")
    
    def _get_cpu_temp(self) -> float:
        """Get CPU temperature in Celsius (cached, updated once per minute)."""
        self._update_system_metrics_cache()
        return self._system_metrics_cache['cpu_temp']
    
    def _get_cpu_usage(self) -> float:
        """Get CPU usage percentage (cached, updated once per minute)."""
        self._update_system_metrics_cache()
        return self._system_metrics_cache['cpu_usage']
    
    def _get_memory_usage(self) -> float:
        """Get memory usage percentage (cached, updated once per minute)."""
        self._update_system_metrics_cache()
        return self._system_metrics_cache['memory_usage']
    
    def _get_disk_usage(self) -> str:
        """Get disk FREE space in MB (cached, updated once per minute)."""
        self._update_system_metrics_cache()
        return self._system_metrics_cache['disk_usage']
    
    def _start_sensor_thread(self):
        """Start background thread for asynchronous sensor reading."""
        if self.simulate:
            return
        
        self._sensor_thread_running = True
        self._sensor_thread = threading.Thread(
            target=self._sensor_update_loop,
            daemon=True,
            name="WeatherSensorUpdateThread"
        )
        self._sensor_thread.start()
        logger.info("Background weather sensor update thread started (updates every 5 seconds)")
    
    def _stop_sensor_thread(self):
        """Stop the background sensor update thread."""
        if self._sensor_thread and self._sensor_thread_running:
            logger.info("Stopping background sensor update thread...")
            self._sensor_thread_running = False
            if self._sensor_thread.is_alive():
                self._sensor_thread.join(timeout=2.0)
            logger.info("Background sensor update thread stopped")
    
    def _sensor_update_loop(self):
        """Background thread loop - continuously updates sensor readings every 5 seconds."""
        logger.info("Weather sensor update loop started")
        
        # Do initial read immediately
        self._update_sensor_cache()
        
        while self._sensor_thread_running:
            try:
                # Sleep in small intervals to allow quick shutdown
                for _ in range(int(self._sensor_update_interval * 10)):
                    if not self._sensor_thread_running:
                        break
                    time.sleep(0.1)
                
                if self._sensor_thread_running:
                    self._update_sensor_cache()
                    
            except Exception as e:
                logger.error(f"Error in weather sensor update loop: {e}")
                time.sleep(1.0)  # Avoid tight error loop
        
        logger.info("Weather sensor update loop exited")
    
    def _update_sensor_cache(self):
        """Update the sensor cache with fresh readings (runs in background thread)."""
        start_time = time.time()
        
        # Initialize values
        temperature = 0.0
        humidity = 0.0
        pressure = 0.0
        device_temperature = 0.0
        dewpoint = 0.0
        lux = 0.0
        
        # Read Weather HAT sensors
        if not self.simulate and hasattr(self, 'weather_hat') and self.weather_hat:
            try:
                # Update the sensor - this might take some time
                self.weather_hat.update(interval=1.0)
                
                # Read values
                temperature = self.weather_hat.temperature  # Celsius
                humidity = self.weather_hat.humidity
                pressure = self.weather_hat.pressure
                device_temperature = self.weather_hat.device_temperature  # Celsius
                dewpoint = self.weather_hat.dewpoint
                lux = self.weather_hat.lux
                
                logger.debug(f"Weather HAT: Temp={temperature}°C, Humidity={humidity}%, "
                           f"Pressure={pressure}hPa, Lux={lux}")
            except Exception as e:
                logger.warning(f"Error reading Weather HAT: {e}")
        
        # Update cache with lock
        elapsed = time.time() - start_time
        with self._sensor_cache_lock:
            self._sensor_cache['temperature'] = temperature
            self._sensor_cache['humidity'] = humidity
            self._sensor_cache['pressure'] = pressure
            self._sensor_cache['device_temperature'] = device_temperature
            self._sensor_cache['dewpoint'] = dewpoint
            self._sensor_cache['lux'] = lux
            self._sensor_cache['last_update'] = time.time()
            self._sensor_cache['update_count'] += 1
        
        logger.debug(f"Weather sensor cache updated in {elapsed:.2f}s (update #{self._sensor_cache['update_count']})")
    
    def read_sensor_data(self) -> Dict:
        """
        Read all sensor data and return as dictionary.
        
        PERFORMANCE: Weather sensors are read asynchronously in a background thread
        every 5 seconds. This method returns the CACHED values instantly.
        
        Returns:
            Dictionary with all sensor readings and metadata (matches weather.py format)
        """
        start_time_dt = datetime.now(timezone.utc)
        start_time = time.time()
        now = datetime.now(timezone.utc)
        
        # Get sensor values from cache (instant, no blocking!)
        with self._sensor_cache_lock:
            temperature_c = self._sensor_cache['temperature']
            humidity = self._sensor_cache['humidity']
            pressure = self._sensor_cache['pressure']
            device_temperature_c = self._sensor_cache['device_temperature']
            dewpoint = self._sensor_cache['dewpoint']
            lux = self._sensor_cache['lux']
            cache_age = time.time() - self._sensor_cache['last_update']
        
        # Log if cache is getting stale (should be updated every 5 seconds)
        if cache_age > 10.0:
            logger.warning(f"Weather sensor cache is stale ({cache_age:.1f}s old)")
        
        # Convert temperatures to Fahrenheit
        temperature_f = round(9.0/5.0 * float(temperature_c) + 32, 2)
        device_temperature_f = round(9.0/5.0 * float(device_temperature_c) + 32, 2)
        
        # Get system metrics
        cpu_temp_c = self._get_cpu_temp()
        cpu_temp_f = int(round(cpu_temp_c * 9/5 + 32))
        cpu_usage = self._get_cpu_usage()
        memory_usage = self._get_memory_usage()
        disk_usage = self._get_disk_usage()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Generate unique identifiers (matching weather.py format)
        row_uuid = str(uuid.uuid4())
        timestamp_str = now.strftime("%Y%m%d%H%M%S")
        
        # Random word generator
        import random
        import string
        randomword = ''.join(random.choice(string.ascii_lowercase) for i in range(3))
        unique_id = f"wthr_{randomword}_{timestamp_str}"
        row_id = f"{timestamp_str}_{row_uuid}"
        
        # Construct data record matching weather.py format
        data = {
            "uuid": unique_id,
            "ipaddress": self.ip_address,
            "cputempf": cpu_temp_f,
            "runtime": int(round(elapsed_time)),
            "host": self.hostname,
            "hostname": self.hostname,
            "macaddress": self.mac_address,
            "endtime": str(end_time),
            "te": str(elapsed_time),
            "cpu": round(cpu_usage, 1),
            "diskusage": disk_usage,
            "memory": round(memory_usage, 1),
            "rowid": row_id,
            "systemtime": now.strftime("%m/%d/%Y %H:%M:%S"),
            "ts": int(now.timestamp()),
            "starttime": start_time_dt.strftime("%m/%d/%Y %H:%M:%S"),
            "pressure": round(pressure, 2),
            "temperature": temperature_f,
            "humidity": round(humidity, 2),
            "devicetemperature": device_temperature_f,
            "dewpoint": round(dewpoint, 2),
            "lux": round(lux, 2)
        }
        
        return data
    
    def read_batch(self, count: int = 1, interval: float = 1.0, fast_mode: bool = False) -> list:
        """
        Read multiple sensor readings.
        
        Args:
            count: Number of readings to collect
            interval: Time between readings in seconds (ignored if fast_mode=True)
            fast_mode: If True, collect readings as fast as possible with minimal delay
            
        Returns:
            List of sensor data dictionaries
        """
        batch = []
        # Fast mode: minimal delay between readings for maximum throughput
        actual_interval = 0.05 if fast_mode else interval  # 50ms in fast mode
        
        for i in range(count):
            data = self.read_sensor_data()
            batch.append(data)
            
            if i < count - 1:  # Don't sleep after last reading
                time.sleep(actual_interval)
        
        return batch
    
    def cleanup(self):
        """Cleanup resources, stop background threads."""
        logger.info("Cleaning up weather sensor...")
        self._stop_sensor_thread()
        logger.info("Weather sensor cleanup complete")
    
    def __del__(self):
        """Destructor - ensure cleanup happens."""
        try:
            self.cleanup()
        except:
            pass  # Avoid errors during garbage collection


def main():
    """Test weather sensor reading with performance measurement."""
    logging.basicConfig(level=logging.INFO)
    
    logger.info("Testing Weather Sensor Reader with Asynchronous Updates")
    logger.info("=" * 70)
    
    # Initialize sensor (will auto-detect or simulate)
    sensor = WeatherSensor()
    
    logger.info(f"Sensor mode: {'REAL SENSORS' if not sensor.simulate else 'SIMULATION'}")
    logger.info("Background thread will update sensors every 5 seconds")
    logger.info("read_sensor_data() returns cached values INSTANTLY (no blocking!)")
    logger.info("=" * 70)
    
    # Wait for first sensor update
    logger.info("Waiting 2 seconds for initial sensor readings...")
    time.sleep(2)
    
    # Test rapid reading performance
    logger.info("\nTesting rapid sensor reads (should be FAST now!):")
    start = time.time()
    
    for i in range(10):
        data = sensor.read_sensor_data()
        
        if i == 0:
            # Print first sample
            logger.info(f"\nSample data:")
            logger.info(f"  Temperature: {data['temperature']:.2f}°F")
            logger.info(f"  Humidity: {data['humidity']:.1f}%")
            logger.info(f"  Pressure: {data['pressure']:.2f} hPa")
            logger.info(f"  Lux: {data['lux']:.2f}")
            logger.info(f"  Dewpoint: {data['dewpoint']:.2f}°F")
            logger.info(f"  CPU Temp: {data['cputempf']:.1f}°F")
            logger.info(f"  CPU Usage: {data['cpu']:.1f}%")
            print(json.dumps(data, indent=2))
        
        time.sleep(0.1)  # Small delay between reads
    
    elapsed = time.time() - start
    rate = 10 / elapsed
    
    logger.info(f"\nPerformance:")
    logger.info(f"  10 readings in {elapsed:.2f} seconds")
    logger.info(f"  Rate: {rate:.2f} rows/sec")
    logger.info(f"  Average time per read: {elapsed/10*1000:.1f} ms")
    
    if rate > 5.0:
        logger.info(f"  SUCCESS! Fast reads enabled (> 5 rows/sec)")
    else:
        logger.info(f"  Performance could be better")
    
    # Cleanup
    sensor.cleanup()


if __name__ == '__main__':
    main()

