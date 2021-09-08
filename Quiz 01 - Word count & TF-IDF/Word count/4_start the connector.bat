cd C:\src\kafka-2.7.0-src

START /b /wait cmd /C curl -d @"c:\my_config\source1.json" -H "Content-Type: application/json" -X POST http://localhost:8081/connectors

pause
