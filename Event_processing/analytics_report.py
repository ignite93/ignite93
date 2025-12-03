import psycopg2
from psycopg2.extras import DictCursor


# Connect to Postgres database
# نوصل على داتابيس بوستجريس
def get_connection():
    return psycopg2.connect(
        dbname="thmanyah_db",   # your DB name
        user="Aziz8",           # اسم المستخدم في بوستجريس
        password="Aziz8",       # الباسورد حق بوستجريس
        host="localhost",       # احنا نكلم الكونتينر عن طريق بورت 5432 على localhost
        port=5432
    )


# Run a query and return all rows
# فنكشن صغيرة تشغل الكويري وترجع النتائج
def run_query(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


# Print section title nicely
# دالة بسيطة تطبع عنوان سيكشن بشكل مرتب
def print_section(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# Pretty print rows from a query
# دالة تطبع الصفوف بشكل بسيط وواضح
def print_rows(rows, headers):
    # Print header row
    # نطبع الهيدر
    header_line = " | ".join(headers)
    print(header_line)
    print("-" * len(header_line))

    # Print each row
    # نلف على الصفوف ونطبعها
    for row in rows:
        print(" | ".join(str(value) for value in row))


def main():
    # Connect to database
    # نفتح اتصال مع قاعدة البيانات
    conn = get_connection()
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        # 1) Events per event_type
        # ١) عدد الأحداث لكل نوع event_type
        print_section("Events per event_type")

        events_per_type_query = """
            SELECT event_type, COUNT(*) AS total_events
            FROM engagement_events
            GROUP BY event_type
            ORDER BY total_events DESC;
        """
        rows = run_query(cur, events_per_type_query)
        print_rows(rows, headers=["event_type", "total_events"])

        # 2) Top content by total engagement events
        # ٢) أكثر محتويات عليها تفاعل (كل الأنواع مع بعض)
        print_section("Top content by total engagement (all event types)")

        top_content_query = """
            SELECT
                content_id,
                COUNT(*) AS total_events
            FROM engagement_events
            GROUP BY content_id
            ORDER BY total_events DESC
            LIMIT 10;
        """
        rows = run_query(cur, top_content_query)
        print_rows(rows, headers=["content_id", "total_events"])

        # 3) Completion rate per content (finish / play)
        # ٣) نسبة الإكمال لكل محتوى (finish / play)
        print_section("Completion rate per content (finish / play)")

        completion_query = """
            WITH plays AS (
                SELECT content_id, COUNT(*) AS total_plays
                FROM engagement_events
                WHERE event_type = 'play'
                GROUP BY content_id
            ),
            finishes AS (
                SELECT content_id, COUNT(*) AS total_finishes
                FROM engagement_events
                WHERE event_type = 'finish'
                GROUP BY content_id
            )
            SELECT
                p.content_id,
                p.total_plays,
                COALESCE(f.total_finishes, 0) AS total_finishes,
                ROUND(
                    COALESCE(f.total_finishes::numeric / NULLIF(p.total_plays, 0), 0) * 100,
                    1
                ) AS completion_rate_percent
            FROM plays p
            LEFT JOIN finishes f USING (content_id)
            ORDER BY completion_rate_percent DESC, total_plays DESC
            LIMIT 10;
        """
        rows = run_query(cur, completion_query)
        print_rows(
            rows,
            headers=["content_id", "total_plays", "total_finishes", "completion_%"]
        )

        # 4) Events per device
        # ٤) توزيع الأحداث حسب نوع الجهاز device
        print_section("Events per device")

        device_query = """
            SELECT
                device,
                COUNT(*) AS total_events
            FROM engagement_events
            GROUP BY device
            ORDER BY total_events DESC;
        """
        rows = run_query(cur, device_query)
        print_rows(rows, headers=["device", "total_events"])

        print("\nDone. Analytics report printed successfully ✅")
        # طبعنا التقرير بنجاح ✅

    finally:
        # Always close cursor and connection
        # دايمًا نقفل الكيرسر والكونكشن بعد ما نخلص
        cur.close()
        conn.close()


if __name__ == "__main__":
    # Entry point of the script
    # هذي نقطة تشغيل السكربت
    main()

