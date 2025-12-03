import psycopg2
from psycopg2.extras import Json
import random
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker

fake = Faker()


def connect_to_db():
    # Create a connection to the PostgreSQL database
    # هنا نسوي اتصال بقاعدة البيانات PostgreSQL
    return psycopg2.connect(
        dbname="thmanyah_db",
        user="Aziz8",
        password="Aziz8",
        host="localhost",
        port=5432,
    )


def generate_sample_content(cur, count=5):
    # Generate some sample content rows and insert them into the "content" table
    # هنا نولد بيانات تجريبية للكونتنت ونحطها في جدول content
    content_types = ["podcast", "newsletter", "video"]
    created_items = []

    for _ in range(count):
        # Create random content fields (id, slug, title, type, length)
        # هنا نكوّن بيانات عشوائية لكل كونتنت (آي دي، سلق، عنوان، نوع، مدة)
        item_id = str(uuid.uuid4())
        item_slug = fake.slug()
        item_title = fake.sentence(nb_words=4)
        item_type = random.choice(content_types)
        length_seconds = random.randint(60, 3000)

        # Insert the content row into the database
        # هنا ندخل الكونتنت في قاعدة البيانات
        cur.execute(
            """
            INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                item_id,
                item_slug,
                item_title,
                item_type,
                length_seconds,
                datetime.now(timezone.utc),
            ),
        )

        # Save the content id and length for later use when generating events
        # نحفظ آي دي الكونتنت والمدة عشان نستخدمها بعدين بالأحداث
        created_items.append((item_id, length_seconds))

    # Return the list of created content items
    # نرجّع ليست بالكونتنت اللي سويناه
    return created_items


def generate_engagement_events(cur, content_list, event_count=20):
    # Generate random engagement events for the existing content
    # هنا نولد أحداث تفاعل عشوائية للكونتنت اللي عندنا
    event_types = ["play", "pause", "finish", "click"]

    for _ in range(event_count):
        # Pick a random content item from the list
        # نختار كونتنت عشوائي من اللي فوق
        content_id, length_seconds = random.choice(content_list)

        # Create random duration and event type
        # نحدد مدة عشوائية ونوع حدث عشوائي
        duration_ms = random.randint(300, 5000)
        event_type = random.choice(event_types)

        # Insert the engagement event into the database
        # ندخل حدث التفاعل في جدول engagement_events
        cur.execute(
            """
            INSERT INTO engagement_events
            (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                content_id,
                str(uuid.uuid4()),  # random user id
                # آي دي مستخدم عشوائي
                event_type,
                datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 300)),
                # نحط وقت الحدث كأنه صار قبل شوي (آخر ٥ دقايق)
                duration_ms,
                random.choice(["ios", "android", "web-safari", "chrome"]),
                # نختار جهاز عشوائي للمستخدم
                Json({"info": "auto-generated"}),
                # نخزّن بيانات خام بسيطة كـ JSON
            ),
        )


def main():
    # Open a connection and create a cursor
    # نفتح اتصال بقاعدة البيانات ونجيب cursor عشان ننفذ أوامر SQL
    conn = connect_to_db()
    cur = conn.cursor()

    # First, create some sample content rows
    # أول شيء نضيف شوية كونتنت تجريبي
    print("→ Adding example content...")
    content_rows = generate_sample_content(cur, count=5)

    # Then, generate engagement events for that content
    # بعدين نولد أحداث تفاعل على نفس الكونتنت
    print("→ Generating engagement events...")
    generate_engagement_events(cur, content_rows, event_count=50)

    # Commit changes and close the connection
    # نحفظ التغييرات في قاعدة البيانات ونقفل الاتصال
    conn.commit()
    cur.close()
    conn.close()

    # Final success message
    # هنا نطبع رسالة إن كل شيء خلص تمام
    print("✓ Done. Sample content + events inserted.")


if __name__ == "__main__":
    # Run the main function when this file is executed directly
    # إذا شغّلنا الملف مباشرة، يشغّل لنا دالة main
    main()
