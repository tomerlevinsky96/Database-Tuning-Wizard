def calculate_postgresql_configuration_parameters(total_memory,DataStorage):
  """Calculates the PostgreSQL configuration parameters according to the given parameters."""

  # Get the default values for the configuration parameters.
  default_parameters = {
      "shared_buffers": '25GB',
      "effective_cache_size":'75GB',
      "maintenance_work_mem": '2GB',
      "checkpoint_completion_target": '0.9',
      "wal_buffers": '16MB',
      "default_statistics_target": '100',
      "random_page_cost": '1.1',
      "effective_io_concurrency": '200',
      "work_mem": '13107kB',
      "min_wal_size": '1GB',
      "max_wal_size": '50GB',
      "max_worker_processes":'100',
      "max_parallel_workers_per_gather":'4',
      "max_parallel_workers": '100',
      "max_parallel_maintenance_workers":'4',
  }

  # Update the default values with the given parameters.
  parameters = default_parameters.copy()
  numeric_part = int(''.join(filter(str.isdigit, total_memory)))
  measurement_size = ''.join(filter(str.isalpha, total_memory))
  new_numeric_value=numeric_part * 0.25
  parameters["shared_buffers"] =f'{new_numeric_value}{measurement_size}'
  new_numeric_value2 = numeric_part * 0.75
  parameters["effective_cache_size"] = f'{new_numeric_value2}{measurement_size}'
  if DataStorage!="SSD":
      parameters["random_page_cost"]="4"


  # Return the calculated configuration parameters.
  return parameters


def ConfigurationParametersTuning(total_memory,DataStorage,file_path):
  parameters = calculate_postgresql_configuration_parameters(total_memory,DataStorage)
  with open(file_path, 'a') as f:
      f.write(f"""\n""")
      f.write(f"""\nParameter configuration tuning suggestions:\n""")
      for parameter, value in parameters.items():
          f.write(f"\n{parameter}: {value}\n")



