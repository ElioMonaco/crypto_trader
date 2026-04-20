import time
import websocket

def run():
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://stream.crypto.com/v2/market",
                on_open=on_open,
                on_message=on_message,
                on_close=on_close,
                on_error=on_error
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("Reconnect due to:", e)

        time.sleep(3)  # backoff