# WAVES Project - Tiến độ dự án

## Thông tin chung

**Đề tài:** WAVES: Watermark-Aware Violation Detection for Streaming Data Quality
**Ngày bắt đầu:** Thursday Apr 9, 2026
**Dataset mẫu:** NYC Taxi Dataset

---

## Cập nhật ngày: 2026-04-09

### Đã hoàn thành ✅

#### 1. Đọc và phân tích Report.pdf
- Nắm bắt định hướng khóa luận
- Hiểu 5 thành phần chính: Stream DaQ, Rapidash, Weever, Watermark, Icewafl
- Hiểu luồng xử lý dữ liệu mới

#### 2. Đọc và phân tích các base projects

**Stream DaQ:**
- Core: StreamDaQ.py, DaQMeasures.py (30+ measures)
- Framework: Pathway (Python stream processing)
- Điểm mạnh: Window-based, dynamic context, meta-stream
- Điểm yếu: Không có complex constraint checking, chưa handle late arrivals

**Rapidash:**
- Core: DCVerifier.java (kd-tree/range-tree)
- Ngôn ngữ: Java 17+
- Phương pháp: Orthogonal range search cho DC violation detection
- Performance: Nhanh hơn SOTA tới 84 lần

**Weever:**
- Core: Weever.java, WeeverSequential.java, WeeverSingleIndex.java
- Chức năng: Incremental index cho streaming
- Giải quyết: Rebuild tree overhead khi window trượt

**Icewafl:**
- Core: PolluterBuilder.py, PolluterBase.py
- Chức năng: Benchmark data generation với pollution
- Framework: Apache Flink

#### 3. Tạo các file cấu hình
- ✅ Tạo `context.md` - Lưu trữ toàn bộ context dự án
- ✅ Tạo `AGENTS.md` - Rules cho AI assistant

### Cấu trúc thư mục đã xác nhận

```
/home/dtl/Documents/thesis/
├── base/
│   ├── stream-DaQ/     # Python (Core base)
│   ├── Rapidash/       # Java (Rapidash paper)
│   ├── Weever/         # Java (Weever paper)
│   └── Icewafl/        # Python (Benchmark generator)
├── paper/              # Các paper liên quan
└── research/           # Paper nghiên cứu thêm
```

---

## Milestones tiếp theo

### Phase 1: Nghiên cứu sâu (Tuần 1-2)
- [ ] Đọc kỹ paper Stream DaQ
- [ ] Đọc kỹ paper Rapidash
- [ ] Đọc kỹ paper Weever
- [ ] Nghiên cứu Watermark mechanism

### Phase 2: Thiết kế kiến trúc (Tuần 3-4)
- [ ] Thiết kế integration giữa Stream DaQ và Rapidash
- [ ] Thiết kế incremental kd-tree cho streaming
- [ ] Thiết kế 2-phase alerting system

### Phase 3: Implementation (Tuần 5-8)
- [ ] Python-Java interop cho Rapidash integration
- [ ] Implement Watermark mechanism
- [ ] Implement incremental index
- [ ] Test với NYC Taxi dataset

### Phase 4: Benchmark và đánh giá (Tuần 9-12)
- [ ] Benchmark với các baseline (Deequ, COD3)
- [ ] Đánh giá performance
- [ ] Viết báo cáo

---

## Issues và Questions cần giải quyết

1. **Python-Java Interop:** Cần nghiên cứu cách Stream DaQ (Python) gọi Rapidash (Java)
   - PyJNIus? Jpype? Subprocess?
   
2. **Incremental KD-Tree:** Weever dùng index riêng, cần design kd-tree increment
   - Tham khảo: Có paper nào về incremental kd-tree không?

3. **Watermark trong Pathway:** Stream DaQ dùng Pathway
   - Xem Pathway có hỗ trợ watermark không?

---

## Ghi chú quan trọng

- **KHÔNG XÓA** bất kỳ file nào khi chưa được phép
- Cập nhật file này mỗi khi có thay đổi
- Research kỹ TRƯỚC KHI implement
