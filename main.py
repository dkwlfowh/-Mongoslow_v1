import json
import tkinter as tk
from tkinter import ttk, filedialog
import dateutil.parser
import pandas as pd
from collections import Counter
import pyperclip
from concurrent.futures import ThreadPoolExecutor
import matplotlib
matplotlib.use("TkAgg")
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# ---------- 유틸리티 함수 ----------

def convert_to_datetime(timestamp_str):
    try:
        timestamp = dateutil.parser.parse(timestamp_str)
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def parse_log_file(log_file):
    """개별 로그 파일에서 Slow Query만 파싱"""
    results = []
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            if '"Slow query"' not in line:
                continue  # 빠른 필터링
            try:
                log_data = json.loads(line)
                if log_data.get('msg') != 'Slow query':
                    continue
                attr = log_data.get('attr', {})

                # durationMillis가 없으면 millis를 사용
                duration = attr.get('durationMillis')
                if duration is None:
                    duration = attr.get('millis', 0)

                results.append({
                    'Timestamp': convert_to_datetime(log_data.get('t', {}).get('$date')),
                    'Duration (ms)': round(duration),
                    'Namespace': attr.get('ns', 'Unknown'),
                    'originatingCommand': attr.get('originatingCommand', attr.get('command', {})),
                    'PlanSummary': attr.get('planSummary')
                })
            except json.JSONDecodeError:
                continue
    return results

# ---------- 로그 처리 ----------

def select_log_files():
    file_paths = filedialog.askopenfilenames(title="Select MongoDB Log Files",
                                             filetypes=[("All Files", "*.*"), ("Log Files", "*.log")])
    if file_paths:
        process_log_files(file_paths)

def process_log_files(log_files):
    """멀티스레드로 로그 병렬 처리 후 GUI 업데이트"""
    all_data = []
    with ThreadPoolExecutor() as executor:
        futures = executor.map(parse_log_file, log_files)
        for result in futures:
            all_data.extend(result)

    global df
    df = pd.DataFrame(all_data)
    update_gui(df)
    draw_scatter_plot(df)


# ---------- GUI 업데이트 ----------

def update_gui(df):
    for item in tree.get_children():
        tree.delete(item)
    for _, row in df.iterrows():
        tree.insert("", "end", values=row.tolist())

    count = Counter(df['Namespace'])

    for widget in button_frame.winfo_children():
        widget.destroy()

    # "All" 버튼
    tk.Button(button_frame, text="All", command=lambda: [update_gui(df), draw_scatter_plot(df, "All")]).pack(side="left", padx=5)

    # 네임스페이스별 버튼
    for ns, cnt in count.items():
        tk.Button(button_frame, text=f"{ns} ({cnt})", command=lambda ns=ns: filter_by_namespace(ns)).pack(side="left", padx=5)

def filter_by_namespace(namespace):
    filtered_df = df[df['Namespace'] == namespace]
    for item in tree.get_children():
        tree.delete(item)
    for _, row in filtered_df.iterrows():
        tree.insert("", "end", values=row.tolist())
    draw_scatter_plot(filtered_df, namespace)


# ---------- GUI 이벤트 ----------

def on_row_double_click(event):
    selected_item = tree.selection()
    if selected_item:
        row_values = tree.item(selected_item)['values']
        filtered_row = df[
            (df["Timestamp"] == row_values[0]) &
            (df["Duration (ms)"] == row_values[1]) &
            (df["Namespace"] == row_values[2]) &
            (df["originatingCommand"].astype(str) == str(row_values[3])) &
            (df["PlanSummary"].astype(str) == str(row_values[4]))
        ]
        if not filtered_row.empty:
            command_data = filtered_row.iloc[0]['originatingCommand']
            formatted_json = json.dumps(command_data, indent=4, ensure_ascii=False) if isinstance(command_data, dict) else str(command_data)

            popup = tk.Toplevel(root)
            popup.title("originatingCommand Details")
            popup.geometry("600x800")

            text = tk.Text(popup, wrap="word", font=("Arial", 10))
            text.pack(expand=True, fill="both", padx=10, pady=10)
            text.insert("1.0", formatted_json)
            text.config(state="disabled")


def sort_by_column(col):
    col_index = df.columns.get_loc(col)
    current_order = sort_order.get(col, 'asc')
    reverse = current_order != 'asc'
    sorted_items = sorted(tree.get_children(), key=lambda item: tree.set(item, col), reverse=reverse)
    sort_order[col] = 'desc' if current_order == 'asc' else 'asc'
    for item in sorted_items:
        tree.move(item, '', len(tree.get_children('')))


def draw_scatter_plot(dataframe, namespace_name="All"):
    dataframe['Timestamp_dt'] = pd.to_datetime(dataframe['Timestamp'], errors='coerce')
    fig = Figure(figsize=(10, 4), dpi=100)
    ax = fig.add_subplot(111)
    ax.scatter(dataframe['Timestamp_dt'], dataframe['Duration (ms)'], alpha=0.7, marker='o')
    ax.set_title(f'Slow Query Duration Over Time - {namespace_name}')
    ax.set_xlabel('Timestamp')
    ax.set_ylabel('Duration (ms)')
    ax.grid(True)

    for widget in frame_plot.winfo_children():
        widget.destroy()

    canvas = FigureCanvasTkAgg(fig, master=frame_plot)
    canvas.draw()
    canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)


# ---------- GUI 초기화 ----------

root = tk.Tk()
root.title("Slow Queries Viewer")
root.resizable(True, True)

file_select_button = tk.Button(root, text="파일선택", command=select_log_files)
file_select_button.pack(side="top", pady=5)

button_frame = tk.Frame(root)
button_frame.pack(side="top", fill="x", pady=5)

notebook = ttk.Notebook(root)
notebook.pack(expand=True, fill='both', padx=10, pady=5)

frame_table = tk.Frame(notebook)
notebook.add(frame_table, text="Slow Query Table")

frame_plot = tk.Frame(notebook)
notebook.add(frame_plot, text="Scatter Plot (Duration vs Time)")

tree = ttk.Treeview(frame_table, columns=["Timestamp", "Duration (ms)", "Namespace", "originatingCommand", "PlanSummary"], show="headings", selectmode="browse")
for col in ["Timestamp", "Duration (ms)", "Namespace", "originatingCommand", "PlanSummary"]:
    tree.heading(col, text=col, command=lambda _col=col: sort_by_column(_col))
    tree.column(col, anchor="center")

tree.bind("<Double-1>", on_row_double_click)

scrollbar_y = tk.Scrollbar(frame_table, orient="vertical", command=tree.yview)
scrollbar_x = tk.Scrollbar(frame_table, orient="horizontal", command=tree.xview)
tree.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)

tree.grid(row=0, column=0, sticky="nsew")
scrollbar_y.grid(row=0, column=1, sticky="ns")
scrollbar_x.grid(row=1, column=0, sticky="ew")

frame_table.grid_rowconfigure(0, weight=1)
frame_table.grid_columnconfigure(0, weight=1)

sort_order = {}
root.mainloop()