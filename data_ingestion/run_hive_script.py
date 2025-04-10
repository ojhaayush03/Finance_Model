import subprocess

hive_script = "../hadoop_scripts/create_stock_table.hql"

print("🚀 Running Hive script to create table...")

# Run through cmd.exe so it uses Windows PATH context
subprocess.run(f'cmd /c "hive -f {hive_script}"', shell=True, check=True)

print("✅ Hive script executed.")
