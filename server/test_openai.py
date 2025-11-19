import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

key = os.getenv("OPENAI_API_KEY")
print("KEY SET:", bool(key), "PREFIX:", key[:7] if key else None)

client = OpenAI(api_key=key)

resp = client.chat.completions.create(
    model=os.getenv("AI_MODEL", "gpt-4o-mini"),
    messages=[
        {"role": "system", "content": "You are a test assistant."},
        {"role": "user", "content": "Скажи 'привет' одним словом."}
    ]
)

print("RAW:", resp.choices[0].message.content)
