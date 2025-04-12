import os
from sensor.sensor import publish_sensor_data


def main():
    """
    Main entry point for the script.

    Publishes sensor data for one or more sensors. To publish data for additional
    sensors, uncomment the corresponding lines.

    """
    
    producers = os.getenv('KAFKA_PRODUCERS_COUNT',1)
    
    for producer in range(int(producers)):
        publish_sensor_data(sensor_id=f'sensor-00{producer}')
    # publish_sensor_data(sensor_id='sensor-003')
    # publish_sensor_data(sensor_id='sensor-004')
    # publish_sensor_data(sensor_id='sensor-005')
    # publish_sensor_data(sensor_id='sensor-006')
    # publish_sensor_data(sensor_id='sensor-007')
    # publish_sensor_data(sensor_id='sensor-008')
    # publish_sensor_data(sensor_id='sensor-009')
    # publish_sensor_data(sensor_id='sensor-010')
    
if __name__ == "__main__":
  main()