# [original]stream-DaQ - Phiên bản gốc (Backup)

Đây là **bản sao** của Stream DaQ gốc từ `base/stream-DaQ/`.

## Mục đích

Dùng để:
- So sánh với WAVES implementation
- Backup trước khi modify
- Tham khảo code gốc

## Yêu cầu hệ thống

```
Python >= 3.11 (BẮT BUỘC)
```

Kiểm tra version:
```bash
python3 --version
```

## Cài đặt Python 3.11+

### Trên Linux (Ubuntu/Debian)

```bash
# Cài đặt pyenv
curl https://pyenv.run | bash

# Thêm vào ~/.bashrc
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc

# Reload shell
source ~/.bashrc

# Cài đặt Python 3.11
pyenv install 3.11.9
pyenv install 3.12.3

# Đặt global version
pyenv global 3.12.3

# Verify
python3 --version
```

### Trên Windows

```powershell
# Sử dụng py launcher hoặc cài Python 3.11+ trực tiếp
# Download từ: https://www.python.org/downloads/
```

### Trên macOS

```bash
# Sử dụng Homebrew
brew install pyenv
brew install python@3.11
brew install python@3.12

# Thêm vào ~/.zshrc
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc
```

## Cài đặt Stream DaQ

### Cách 1: Cài từ PyPI (khuyến nghị)

```bash
# Tạo virtual environment với Python 3.11+
python3.11 -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate     # Windows

# Cài đặt
pip install streamdaq
```

### Cách 2: Cài từ source (development)

```bash
# Clone repository
git clone https://github.com/Bilpapster/stream-DaQ.git
cd stream-DaQ

# Tạo virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Cài đặt với dev dependencies
pip install -e ".[dev]"
```

### Cách 3: Sử dụng local folder này

```bash
cd "[original]stream-DaQ"

# Tạo virtual environment với Python 3.11+
python3.11 -m venv .venv
source .venv/bin/activate

# Cài đặt local package
pip install -e .
```

## Chạy ví dụ

```bash
# Activate environment
source .venv/bin/activate

# Chạy ví dụ cơ bản
cd examples
python basic.py
```

## Các ví dụ có sẵn

```
examples/
├── basic.py                    # Ví dụ đơn giản nhất
├── anomaly_detection.py         # Phát hiện bất thường
├── trend_analysis.py           # Phân tích xu hướng
├── schema_validation.py        # Validation schema
├── mixed_api_usage.py          # Sử dụng API hỗn hợp
├── multi_source_monitoring.py   # Monitoring nhiều nguồn
└── custom_connector.py         # Custom connector
```

## Dependencies chính

Từ `pyproject.toml`:

| Package | Version | Purpose |
|---------|---------|---------|
| pathway | ~0.26.4 | Stream processing |
| matplotlib | 3.10.1 | Visualization |
| python-dateutil | 2.9.0 | Date parsing |
| datasketch | 1.6.5 | Approximate counts |
| datasketches | 5.1.0 | Advanced sketches |

## Troubleshooting

### Lỗi: Python version thấp

```bash
# Kiểm tra Python version
python3 --version
# Phải là 3.11 hoặc cao hơn
```

### Lỗi: Import pathway thất bại

```bash
# Cài đặt pathway riêng
pip install pathway==0.26.4
```

### Lỗi: Module not found 'streamdaq'

```bash
# Chạy từ thư mục cha
cd ..
python -c "from streamdaq import StreamDaQ"
```

## Cross-Platform Notes

### Linux/Windows path compatibility

Sử dụng `pathlib` thay vì hardcoded paths:

```python
from pathlib import Path

# ✅ Đúng
base_dir = Path(__file__).parent
data_file = base_dir / "data" / "input.csv"

# ❌ Sai
data_file = "data/input.csv"  # Có thể lỗi trên Windows
```

### OS detection

```python
import sys
import platform

if sys.platform == "linux":
    print("Linux")
elif sys.platform == "win32":
    print("Windows")
elif sys.platform == "darwin":
    print("macOS")
```

## File structure

```
[original]stream-DaQ/
├── pyproject.toml              # Dependencies (dùng pyproject.toml!)
├── README.md                   # File này
├── streamdaq/                  # Source code
│   ├── __init__.py
│   ├── StreamDaQ.py           # Core class
│   ├── DaQMeasures.py         # 50+ DQ measures
│   ├── Windows.py            # Windowing
│   ├── Task.py               # Task management
│   ├── SchemaValidator.py    # Pydantic validation
│   ├── CustomReducers.py      # HLL++, etc.
│   ├── anomaly_detectors/     # Bonus features
│   └── ...
├── examples/                  # Ví dụ
├── docs/                     # Documentation
└── scripts/                  # Utility scripts
```

## Notes

- Đây là **bản backup** của Stream DaQ gốc
- **KHÔNG sửa đổi** trong folder này
- Mọi thay đổi nên được thực hiện trong `base/stream-DaQ/`
