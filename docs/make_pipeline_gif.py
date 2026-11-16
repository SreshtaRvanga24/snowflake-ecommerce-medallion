from PIL import Image, ImageDraw, ImageFont
import math

W, H = 1400, 640
F = "/usr/share/fonts/opentype/inter/"
f_title = ImageFont.truetype(F + "Inter-Bold.otf", 26)
f_box = ImageFont.truetype(F + "Inter-SemiBold.otf", 17)
f_small = ImageFont.truetype(F + "Inter-Regular.otf", 13)
f_cap = ImageFont.truetype(F + "Inter-SemiBold.otf", 19)
f_layer = ImageFont.truetype(F + "Inter-Bold.otf", 13)

BG = (250, 250, 248)
INK = (34, 38, 46)
MUTED = (110, 116, 128)
LINE = (196, 200, 208)
BOX = (255, 255, 255)
ACCENT = (41, 98, 255)
ACCENT_FILL = (232, 239, 255)
DONE_FILL = (238, 247, 241)
DONE = (46, 140, 87)
DOT = (255, 120, 40)

# boxes: key -> (x, y, w, h, title, subtitle)
B = {
    "gen":    (40, 250, 170, 74, "Daily generator", "Python, 21:00 IST"),
    "azure":  (260, 150, 180, 74, "Azure Blob", "Azurite emulator"),
    "s3":     (260, 350, 180, 74, "Amazon S3", "landing bucket"),
    "ing_az": (490, 150, 190, 74, "Airflow + Python", "PUT → internal stage"),
    "ing_s3": (490, 350, 190, 74, "Snowflake procedure", "COPY ← external stage"),
    "bronze": (720, 250, 170, 74, "BRONZE", "raw rows + lineage"),
    "silver": (930, 250, 170, 74, "SILVER", "dbt · tests · SCD2"),
    "gold":   (1140, 140, 200, 74, "GOLD", "star schema (Task)"),
    "report": (1140, 280, 200, 74, "REPORTING", "dynamic tables, views"),
    "dash":   (1140, 420, 200, 74, "Dashboard", "Streamlit · planned"),
}
EDGES = [("gen", "azure"), ("gen", "s3"), ("azure", "ing_az"), ("s3", "ing_s3"),
         ("ing_az", "bronze"), ("ing_s3", "bronze"), ("bronze", "silver"),
         ("silver", "gold"), ("gold", "report"), ("report", "dash")]

STEPS = [
    (["gen"], [], "1  Generate today's batch: new customers, orders, payments, status events"),
    (["azure", "s3"], [("gen", "azure"), ("gen", "s3")], "2  Upload: payments & events to Azure Blob · customers, orders, items to S3"),
    (["ing_az", "ing_s3"], [("azure", "ing_az"), ("s3", "ing_s3")], "3  Ingest two ways: Airflow-orchestrated vs Snowflake-native, both idempotent"),
    (["bronze"], [("ing_az", "bronze"), ("ing_s3", "bronze")], "4  Bronze: every row exactly as received, with file and load lineage"),
    (["silver"], [("bronze", "silver")], "5  Silver: dbt types, deduplicates, merges only real changes, snapshots SCD2"),
    (["gold"], [("silver", "gold")], "6  Gold: Snowflake task rebuilds the star schema with point-in-time customers"),
    (["report"], [("gold", "report")], "7  Reporting: dynamic tables and KPI views, reconciled against Silver"),
]


def anchor(key, side):
    x, y, w, h, *_ = B[key]
    return {"r": (x + w, y + h / 2), "l": (x, y + h / 2), "b": (x + w / 2, y + h), "t": (x + w / 2, y)}[side]


def edge_points(a, b):
    ax, ay, aw, ah, *_ = B[a]
    bx, by, bw, bh, *_ = B[b]
    if a in ("gold", "report"):                       # vertical chain on the right
        return [anchor(a, "b"), anchor(b, "t")]
    p0, p3 = anchor(a, "r"), anchor(b, "l")
    if abs(p0[1] - p3[1]) < 1:
        return [p0, p3]
    mx = (p0[0] + p3[0]) / 2                          # elbow connector
    return [p0, (mx, p0[1]), (mx, p3[1]), p3]


def point_along(pts, t):
    segs = [(pts[i], pts[i + 1]) for i in range(len(pts) - 1)]
    lens = [math.dist(a, b) for a, b in segs]
    d = t * sum(lens)
    for (a, b), L in zip(segs, lens):
        if d <= L:
            r = d / L if L else 0
            return (a[0] + (b[0] - a[0]) * r, a[1] + (b[1] - a[1]) * r)
        d -= L
    return pts[-1]


def draw_arrow(d, pts, color, width):
    d.line(pts, fill=color, width=width, joint="curve")
    (x1, y1), (x2, y2) = pts[-2], pts[-1]
    ang = math.atan2(y2 - y1, x2 - x1)
    s = 9
    d.polygon([(x2, y2),
               (x2 - s * math.cos(ang - 0.45), y2 - s * math.sin(ang - 0.45)),
               (x2 - s * math.cos(ang + 0.45), y2 - s * math.sin(ang + 0.45))], fill=color)


def frame(step_idx, t, done_boxes, done_edges):
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    d.text((40, 28), "E-commerce medallion pipeline on Snowflake", font=f_title, fill=INK)
    d.text((40, 64), "Daily, automated, end to end  ·  Airflow · dbt · Snowflake Tasks & Dynamic Tables",
           font=f_small, fill=MUTED)

    # layer labels
    for label, x in [("SOURCES", 40), ("LANDING", 260), ("INGESTION", 490), ("BRONZE · SILVER", 720), ("GOLD · CONSUME", 1140)]:
        d.text((x, 108), label, font=f_layer, fill=MUTED)
    d.line([(40, 128), (W - 40, 128)], fill=LINE, width=1)

    active_boxes, active_edges, caption = STEPS[step_idx] if step_idx >= 0 else ([], [], "")
    for a, b in EDGES:
        pts = edge_points(a, b)
        if (a, b) in active_edges:
            draw_arrow(d, pts, ACCENT, 3)
        elif (a, b) in done_edges:
            draw_arrow(d, pts, DONE, 2)
        else:
            draw_arrow(d, pts, LINE, 2)

    for key, (x, y, w, h, title, sub) in B.items():
        is_active = key in active_boxes and t >= 0.95
        if is_active:
            fill, outline, ow = ACCENT_FILL, ACCENT, 3
        elif key in done_boxes:
            fill, outline, ow = DONE_FILL, DONE, 2
        else:
            fill, outline, ow = BOX, LINE, 2
        if key == "dash" and key not in done_boxes and not is_active:
            d.rounded_rectangle([x, y, x + w, y + h], 12, fill=fill)
            for i in range(x, x + w, 12):                     # dashed outline: planned
                d.line([(i, y), (min(i + 6, x + w), y)], fill=outline, width=2)
                d.line([(i, y + h), (min(i + 6, x + w), y + h)], fill=outline, width=2)
            for j in range(y, y + h, 12):
                d.line([(x, j), (x, min(j + 6, y + h))], fill=outline, width=2)
                d.line([(x + w, j), (x + w, min(j + 6, y + h))], fill=outline, width=2)
        else:
            d.rounded_rectangle([x, y, x + w, y + h], 12, fill=fill, outline=outline, width=ow)
        d.text((x + w / 2, y + 27), title, font=f_box, fill=INK, anchor="mm")
        d.text((x + w / 2, y + 51), sub, font=f_small, fill=MUTED, anchor="mm")

    # moving dots along active edges
    if t < 0.95:
        for a, b in active_edges:
            px, py = point_along(edge_points(a, b), t / 0.95)
            d.ellipse([px - 7, py - 7, px + 7, py + 7], fill=DOT)

    # footer: orchestration + quality bars
    d.rounded_rectangle([40, 528, 1060, 560], 8, fill=(243, 244, 247))
    d.text((52, 544), "Airflow DAG daily_retail_pipeline orchestrates every step  ·  "
           "audit log per file  ·  dbt tests  ·  Gold reconciliation checks",
           font=f_small, fill=MUTED, anchor="lm")
    d.text((40, 590), caption, font=f_cap, fill=ACCENT)
    return img


frames, durations = [], []
done_boxes, done_edges = set(), set()
for i, (boxes, edges, _) in enumerate(STEPS):
    n = 9 if edges else 1
    for k in range(n):
        t = (k + 1) / n if edges else 1.0
        frames.append(frame(i, t if edges else 1.0, done_boxes, done_edges))
        durations.append(70)
    frames.append(frame(i, 1.0, done_boxes, done_edges))
    durations.append(1300)                       # hold on each completed step
    done_boxes |= set(boxes)
    done_edges |= set(edges)

# final frame: everything done
final = frame(-1, 1.0, done_boxes, done_edges)
d = ImageDraw.Draw(final)
d.rectangle([40, 578, W - 40, 612], fill=BG)
d.text((40, 590), "Done: fresh, tested, reconciled data every evening", font=f_cap, fill=DONE)
frames.append(final)
durations.append(2500)

pal = [f.convert("P", palette=Image.ADAPTIVE, colors=64) for f in frames]
out = "/mnt/user-data/outputs/docs/images/pipeline_flow.gif"
pal[0].save(out, save_all=True, append_images=pal[1:], duration=durations, loop=0, optimize=True)
frames[len(frames) // 2].save("/home/claude/preview_mid.png")
final.save("/home/claude/preview_final.png")
import os
print(len(frames), "frames,", round(os.path.getsize(out) / 1024), "KB")
