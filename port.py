import subprocess
import time
import requests
import tkinter as tk
import threading

root = tk.Tk()
root.geometry("400x150")
root.title("Chess Server Launcher")

text = tk.Text(root, height=6, width=50)
text.pack()

def log(msg):
    text.insert(tk.END, msg + "\n")
    text.see(tk.END)

def start_services():
    log("Starting server and ngrok...")

    # Step 1: Start Python server
    subprocess.Popen(
        ["python", "server.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW
    )
    log("Server started in background...")

    # Step 2: Wait for server
    time.sleep(3)

    # Step 3: Start ngrok
    subprocess.Popen(
        [r"C:\Users\soura\Downloads\ngrok.exe", "http", "5050"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW
    )
    log("Ngrok started in background...")

    # Step 4: Wait for ngrok
    time.sleep(5)

    # Step 5: Fetch public URL
    try:
        res = requests.get("http://127.0.0.1:4040/api/tunnels").json()
        public_url = res['tunnels'][0]['public_url']
        log(f"🌍 Public URL: {public_url}")
    except Exception as e:
        log(f"❌ Could not fetch ngrok URL: {e}")

# Run in separate thread (so GUI doesn't freeze)
threading.Thread(target=start_services, daemon=True).start()

# ✅ Correct: call only once
root.mainloop()