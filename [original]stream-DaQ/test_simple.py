"""Test StreamDaQ với artificial data"""
import pathway as pw
from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
from datetime import timedelta
from streamdaq.artificial_stream_generators import generate_artificial_random_viewership_data_stream as artificial

# Tạo artificial stream
source = artificial(number_of_rows=50, input_rate=1.0)

print("Source created!")
print(f"Source schema: {source.schema}")

# Configure StreamDaQ
daq = StreamDaQ().configure(
    window=Windows.tumbling(3),  # int = 3 seconds
    instance="user_id",
    time_column="timestamp",
    source=source
)

# Thêm checks
daq.add(dqm.count('interaction_events'), assess="> 0", name="count")
daq.add(dqm.max('interaction_events'), assess="> 5", name="max_interact")

# Chạy monitoring (start=False để không blocking)
print("\nRunning StreamDaQ monitoring...")
meta_stream = daq.watch_out(start=False)
pw.run()
