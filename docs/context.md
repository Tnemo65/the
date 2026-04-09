# WAVES: Context của dự án khóa luận tốt nghiệp

## 1. Tổng quan đề tài

**Tên đề tài:** WAVES: Watermark-Aware Violation Detection for Streaming Data Quality

**Mục tiêu:** Xây dựng hệ thống giám sát chất lượng dữ liệu (Data Quality - DQ) thời gian thực trên luồng dữ liệu (data streams), kết hợp khả năng phát hiện vi phạm ràng buộc logic phức tạp với cơ chế xử lý dữ liệu đến trễ (late arrivals).

**Dataset mẫu:** NYC Taxi Dataset

---

## 2. Kiến trúc hệ thống WAVES

```
Stream đầu vào → Stream DaQ (Windowing) → Rapidash + Weever (Quét lỗi logic nhanh) 
                → Watermark (Lọc cảnh báo 2 pha) → Stream DaQ (Meta-stream cảnh báo)
```

### 2.1. Các thành phần chính

| Thành phần | Ngôn ngữ | Chức năng |
|------------|----------|-----------|
| **Stream DaQ** | Python | Windowing, kiểm tra DQ cơ bản, meta-stream |
| **Rapidash** | Java | Kd-tree/Range-tree cho violation detection |
| **Weever** | Java | Incremental index cho streaming |
| **Icewafl** | Python | Benchmark data generation (pollution) |

---

## 3. Chi tiết từng Base Code

### 3.1. Stream DaQ (Base chính để nâng cấp)

**Vị trí:** `/base/stream-DaQ/`

**Framework:** Python 3.11+, sử dụng Pathway library

**Cấu trúc chính:**
- `streamdaq/StreamDaQ.py` - Class chính, quản lý tasks và pipeline
- `streamdaq/DaQMeasures.py` - 30+ loại measurements (min, max, count, mean, median, correlation, trend,...)
- `streamdaq/Task.py` - Quản lý task riêng biệt
- `streamdaq/Windows.py` - Cấu hình window (tumbling, sliding, session)

**Điểm mạnh:**
- Tốc độ nhanh, độ trễ thấp
- Window-based context động (tự động điều chỉnh ngưỡng)
- Quality meta-stream xuất liên tục

**Điểm yếu cần khắc phục:**
- Giảm hiệu suất khi cửa sổ trượt chồng chéo cao (90%)
- Chỉ có phép đo cơ bản, không kiểm tra ràng buộc logic phức tạp chéo dòng
- Chưa có cơ chế xử lý late arrivals

### 3.2. Rapidash (Thành phần tăng tốc)

**Vị trí:** `/base/Rapidash/`

**Ngôn ngữ:** Java 17+

**File quan trọng:**
- `src/main/java/org/dc/DCVerifier.java` - Core violation detection
- `src/main/java/kdrangeDouble/KDTree.java` - Kd-tree implementation
- `src/main/java/rangetree/RangeTreeCount.java` - Range-tree implementation

**Phương pháp:**
- Sử dụng kd-tree/range-tree để tìm violations với O(log n)
- Orthogonal range search trong không gian nhiều chiều
- Hash table kết hợp (mỗi hash bucket = 1 cây)

**Ví dụ DC:**
```
Category = Category
Distance < Fare  → NOT(s.Category = t.Category AND s.Distance < t.Fare)
```

### 3.3. Weever (Thành phần incremental)

**Vị trí:** `/base/Weever/`

**Ngôn ngữ:** Java

**File quan trọng:**
- `src/main/java/de/hpi/isg/Weever.java` - Base class
- `src/main/java/de/hpi/isg/WeeverSingleIndex.java` - Single index optimization
- `src/main/java/de/hpi/isg/WeeverSequential.java` - Sequential processing với scheduling

**Chức năng:**
- Incremental index update (insert/delete)
- Không cần rebuild tree từ đầu khi window trượt
- Quản lý chỉ mục cho bất phương trình

### 3.4. Icewafl (Benchmark Generator)

**Vị trí:** `/base/Icewafl/`

**Ngôn ngữ:** Python (Apache Flink)

**Chức năng:**
- Tạo benchmark data từ seed data
- Pollution process có thể cấu hình
- Hỗ trợ nhiều error patterns (temporal, conditional, probabilistic)

---

## 4. Watermark机制

**Mục đích:** Xử lý dữ liệu đến trễ hoặc sai thứ tự

**Cơ chế:**
- Không chốt kết quả ngay lập tức
- Dùng mốc thời gian quyết định khi nào dữ liệu đủ hoàn chỉnh
- Phát hiện violation → Phán quyết tạm → Chờ `wait_for_late` → Phán quyết cuối

**Ví dụ:**
```
Taxi mất sóng → Tọa độ bị trễ
→ Có watermark: Phán quyết tạm → Taxi có sóng → Cập nhật → Hủy báo động giả
→ Không watermark: Báo "Cảm biến hỏng" (sai)
```

---

## 5. Denial Constraints (DCs)

**Định nghĩa:** Ràng buộc phủ định, dạng `NOT(predicate1 AND predicate2 AND ...)`

**Ví dụ NYC Taxi:**
```
DC1: NOT(s.passenger_count > t.passenger_count AND s.fare < t.fare)
     → Nếu A có nhiều khách hơn B thì A không được có cước phí thấp hơn B
```

**Các loại DC:**
- **Homogeneous:** Cột LHS = Cột RHS (VD: `Distance < Fare`)
- **Heterogeneous:** Cột LHS ≠ Cột RHS (VD: `s.Distance < t.Fare`)
- **Single inequality:** Chỉ 1 điều kiện bất phương trình
- **Multiple inequalities:** Nhiều điều kiện

---

## 6. Cấu trúc thư mục dự án

```
/home/dtl/Documents/thesis/
├── Report.pdf              # Báo cáo định hướng khóa luận
├── project.md               # Tiến độ dự án
├── context.md               # Context file này
├── AGENTS.md                # Rules cho AI agent
│
├── base/
│   ├── stream-DaQ/          # Base chính (Python)
│   │   ├── streamdaq/
│   │   │   ├── StreamDaQ.py
│   │   │   ├── DaQMeasures.py
│   │   │   ├── Task.py
│   │   │   ├── Windows.py
│   │   │   └── ...
│   │   ├── examples/
│   │   ├── docs/
│   │   └── README.md
│   │
│   ├── Rapidash/            # Violation detection (Java)
│   │   ├── src/main/java/
│   │   │   ├── org/dc/
│   │   │   ├── kdrange/
│   │   │   ├── kdrangeDouble/
│   │   │   └── rangetree/
│   │   └── README.md
│   │
│   ├── Weever/              # Incremental index (Java)
│   │   ├── src/main/java/de/hpi/isg/
│   │   ├── configurations/
│   │   └── pom.xml
│   │
│   └── Icewafl/             # Benchmark generator (Python)
│       ├── src/
│       │   ├── Builder/
│       │   ├── Conditions/
│       │   ├── Polluters/
│       │   └── Utils/
│       └── evaluation_helper.py
│
├── paper/
│   ├── Stream DaQ.pdf
│   ├── rapidash.pdf
│   ├── Icewalf.pdf
│   ├── watermark.pdf
│   └── watermark2.pdf
│
└── research/
    ├── edbt.reds.pdf
    ├── 3210284.3210293.pdf
    ├── 2507.20839v1.pdf
    └── 3686592.3686609.pdf
```

---

## 7. Dependencies chính

### Stream DaQ
- Python 3.11+
- Pathway
- NumPy, SciPy

### Rapidash
- Java 17+
- Maven

### Weever
- Java
- Maven
- FastUtil library

### Icewafl
- Python 3.10+
- Apache Flink 1.17.0
- Pandas

---

## 8. Flow xử lý dữ liệu mới (WAVES)

```
1. Stream vào
   ↓
2. Stream DaQ chia thành các window thời gian
   ↓
3. Rapidash + Weever quét lỗi logic nhanh
   - Rapidash: Kd-tree/Range-tree orthogonal range search
   - Weever: Incremental insert/delete
   ↓
4. Watermark lọc cảnh báo 2 pha
   - Tạm thời: Violation được phát hiện nhưng chưa final
   - Chính thức: Sau khi watermark qua, không còn late data
   ↓
5. Stream DaQ xuất meta-stream cảnh báo
```

---

## 9. Các vấn đề cần giải quyết

### 9.1. Hiệu suất Window Sliding
- **Vấn đề:** Overlap 90% → tính toán lặp lại
- **Giải pháp:** Weever incremental index

### 9.2. Late Arrivals
- **Vấn đề:** Dữ liệu đến trễ → báo động giả
- **Giải pháp:** Watermark mechanism

### 9.3. Complex Constraint Checking
- **Vấn đề:** Cross-row violation (DC) cần O(n²) với brute force
- **Giải pháp:** Rapidash kd-tree

---

## 10. Phương pháp nghiên cứu

### Paper liên quan:
1. **Stream DaQ (2025)** - Window-based DQ monitoring
2. **Rapidash (VLDB 2024)** - Efficient DC violation detection với kd-tree
3. **Weever (HPI)** - Incremental constraint monitoring
4. **Watermark Papers** - Late data handling

### Công cụ thực tế so sánh:
- Amazon Deequ (Spark) - Chỉ coi stream như batch nhỏ
- COD3 (2025) - Chỉ hỗ trợ Functional Dependencies

---

## 11. Các bước triển khai tiếp theo

1. [ ] Tích hợp Rapidash vào Stream DaQ (Python-Java interop)
2. [ ] Thiết kế incremental kd-tree cho streaming
3. [ ] Implement Watermark mechanism
4. [ ] Thiết kế 2-phase alerting system
5. [ ] Benchmark với NYC Taxi dataset
6. [ ] So sánh với Deequ, COD3

---

## 12. Ghi chú quan trọng

- **NGHIÊM CẤM XÓA FILE/FOLDER** khi chưa được phép
- Mọi thay đổi phải cập nhật vào `project.md`
- Cần research kỹ trước khi implement, không đoán mò
- Không tự ý tạo "lite version" hoặc "fallback" để tránh khó

---

*Context được tạo: Thursday Apr 9, 2026*
