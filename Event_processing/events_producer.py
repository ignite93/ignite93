from confluent_kafka import Producer
from faker import Faker
import random
import uuid
import json
from datetime import datetime, timezone, timedelta

fake = Faker()

# Kafka broker connection string
# هذا عنوان كافكا اللي السكربت يرسل له
KAFKA_BROKER = "localhost:9092"

# Kafka topic name
# اسم التوبك اللي بنبث فيه الأحداث
TOPIC_NAME = "engagement_stream"


def create_producer():
    """
    Create Kafka producer.
    فانكشن ترجع لنا كافكا بروديوسر جاهز للإرسال
    """
    return Producer({"bootstrap.servers": KAFKA_BROKER})


def generate_event():
    """
    Generate one fake engagement event.
    تسوي حدث واحد وهمي يمثل تفاعل المستخدم
    """
    return {
        "event_id": str(uuid.uuid4()),          # unique id للحدث
        "content_id": str(uuid.uuid4()),        # id للمحتوى
        "user_id": str(uuid.uuid4()),           # id للمستخدم
        "event_type": random.choice(["play", "pause", "finish", "click"]),
        # وقت الحدث (قريب من الآن) بصيغة ISO
        # timestamp قريب من الآن، كأن الحدث صار قبل ثواني
        "event_ts": (
            datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 200))
        ).isoformat(),
        "duration_ms": random.randint(200, 5000),  # مدة التفاعل بالميلي ثانية
        "device": random.choice(["ios", "android", "chrome", "web-safari"]),
        "raw_payload": {"note": "sample-event"},   # حقل إضافي بسيط
    }


def main():
    # Create producer instance
    # نجهز البروديوسر
    producer = create_producer()

    print("→ Streaming events to Kafka... (Ctrl + C to stop)\n")

    # Infinite loop to send events
    # لوب مستمر يرسل أحداث لين نوقفه يدوي
    while True:
        event = generate_event()
        # send event as JSON to Kafka
        # نرسل الحدث على شكل JSON لتوبك كافكا
        producer.produce(TOPIC_NAME, json.dumps(event).encode("utf-8"))
        producer.flush()  # نتأكد انه انرسل

        print("Sent:", event)


if __name__ == "__main__":
    # Script entry point
    # نقطة تشغيل السكربت
    main()
