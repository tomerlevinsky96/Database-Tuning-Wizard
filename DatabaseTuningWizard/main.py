import tkinter as tk
from tkinter import filedialog
import psycopg2
import GenereateIndexes
import GenereatePartitions
import caching
import ConfigurationParametersTuning
import os
import subprocess
import re
from tkinter import scrolledtext
file_path=""
# Create a Tkinter window
window = tk.Tk()
window.title("DB TUNER")
import InsertAuotoQuerriesToDB
import InsertAuotoQueryToDB
import GenereateIndex
# Define function to browse for SQL script file
def browse_file():
    global path, filename, file_path
    file_path = filedialog.askopenfilename()
    script_file_entry.delete(0, tk.END)
    script_file_entry.insert(0, file_path)
    path, filename = os.path.split(file_path)
    print("Path:", path)
    print("Filename:", filename)

def select_option():
    selected_indices = options_listbox.curselection()  # Get the index of the selected item
    if selected_indices:
        selected_index = selected_indices[0]
        selected_option = options[selected_index]
        return selected_option
    else:
        return None

def calculate_total_hit_ratio(conn):
    cursor = conn.cursor()
    query = "SELECT sum(heap_blks_hit) as total_hits, sum(heap_blks_read) as total_reads FROM pg_statio_user_tables"
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        hits = result[0]
        reads = result[1]
        if hits + reads == 0:
            return 0
        total_hit_ratio = hits / (hits + reads)
        return total_hit_ratio
    else:
        return None




# Define function to start the timer
def suggestions():
    flag="true"
    try:
        conn = psycopg2.connect(
            database=DBname.get(),
            user=User.get(),
            password=Password.get(),
            host=Host.get(),
            port=Port.get()
        )
        print("Connection successful!")
        error_label.config(text="")
        file_path = os.path.join(path, filename)
    except:
        error_label.config(text="Connection wasn't succesful,please try again!")
        flag="false"
        print("Unable to connect to the database.")
    if flag=="true":
       global start_time,IndexCreatedArray
       timer_label.config(text="Please wait")
       window.update()
       original_path = os.getcwd()
       with open(file_path, 'w') as f:
           f.write('The recomendaitions:\n')
       DataStorage = select_option()
       conn2, cur = InsertAuotoQuerriesToDB.InsertAuotoQuerriesToDB(Host.get(),Port.get(), DBname.get(), User.get(), Password.get(), original_path)
       GenereateIndexes.GenereateIndexes(Host.get(), User.get(), Port.get(), DBname.get(), Password.get(),file_path,conn2,cur)
       GenereatePartitions.GenereatePartitions(Host.get(),User.get(),Port.get(),DBname.get(),Password.get(),file_path,conn2,cur)
       conn2.close()
       original_path = os.getcwd()
       os.chdir(original_path)
       os.remove('mydatabase.db')
       caching.caching(Host.get(),User.get(),Port.get(), DBname.get(), Password.get(), file_path)
       if DataStorage is not None and memory.get() is not None:
          ConfigurationParametersTuning.ConfigurationParametersTuning(memory.get(),DataStorage,file_path)
       timer_label.config(text="You may now see the suggestions created in the script")
       window.update()




def start_Inexing():
    flag = "true"
    try:
        conn = psycopg2.connect(
            database=DBname.get(),
            user=User.get(),
            password=Password.get(),
            host=Host.get(),
            port=Port.get()
        )
        print("Connection successful!")
        file_path = os.path.join(path, filename)
    except:
        error_label.config(text="Connection wasn't succesful,please try again!")
        flag = "false"
        print("Unable to connect to the database.")
    if flag == "true":
        original_path = os.getcwd()
        timer_label.config(text="Please wait")
        window.update()
        original_path = os.getcwd()
        conn2, cur = InsertAuotoQueryToDB.InsertAuotoQueryToDB(Host.get(), Port.get(), DBname.get(), User.get(),Password.get(),file_path,original_path)
        GenereateIndex.GenereateIndex(Host.get(),User.get(),Port.get(),DBname.get(),Password.get(),file_path,conn2,cur)
        timer_label.config(text="You may now see the index suggestion created in the script")
        conn2.close()
        original_path = os.getcwd()
        os.chdir(original_path)
        os.remove('mydatabase.db')


title_label = tk.Label(window, text="Connect to a database")
title_label.grid(row=6, column=1)


title_label = tk.Label(window, text="DB name")
title_label.grid(row=7, column=0)

DBname= tk.Entry(window)
DBname.grid(row=7, column=1)

title_label = tk.Label(window, text="Host")
title_label.grid(row=8, column=0)

Host = tk.Entry(window)
Host.grid(row=8, column=1)

title_label = tk.Label(window, text="User")
title_label.grid(row=9, column=0)

User = tk.Entry(window)
User.grid(row=9, column=1)

title_label = tk.Label(window, text="Password")
title_label.grid(row=10, column=0)

Password = tk.Entry(window,show="*")
Password.grid(row=10, column=1)

title_label = tk.Label(window, text="Port")
title_label.grid(row=11, column=0)

Port = tk.Entry(window)
Port.grid(row=11, column=1)

timer_label = tk.Label(window, text="")
timer_label.grid(row=19, column=1)

error_label = tk.Label(window, text="")
error_label.grid(row=20, column=1)




script_file_label = tk.Label(window, text="Enter the script to create the suggestions/index for specific query")
script_file_label.grid(row=22, column=0)

script_file_entry = tk.Entry(window)
script_file_entry.grid(row=22, column=1)


script_file_button = tk.Button(window, text="Browse", command=browse_file)
script_file_button.grid(row=22, column=2)

script_file_label = tk.Label(window, text="Option 1:Creating optimization suggestions ")
script_file_label.grid(row=24, column=1)



execution_time = tk.Label(window, text="")
execution_time.grid(row=33, column=1)

title_label = tk.Label(window, text="Optionial:")
title_label.grid(row=35, column=0)

title_label = tk.Label(window, text="Enter how much memory can PostgreSQL use:")
title_label.grid(row=36, column=0)

memory = tk.Entry(window)
memory.grid(row=36, column=1)

title_label = tk.Label(window, text="Enter data storage type:")
title_label.grid(row=37, column=0)


options = ["SSD", "Something else"]

options_listbox = tk.Listbox(window, selectmode=tk.SINGLE, height=len(options))
for option in options:
    options_listbox.insert(tk.END, option)
options_listbox.grid(row=37, column=1)

timer_button = tk.Button(window, text="Start creating suggestions", command=suggestions)
timer_button.grid(row=39, column=1)


script_file_label = tk.Label(window, text="Option 2:Indexes creation for the query of the User")
script_file_label.grid(row=40, column=1)


timer_button = tk.Button(window, text="Start creating index/es", command=start_Inexing)
timer_button.grid(row=41, column=1)









window.mainloop()





