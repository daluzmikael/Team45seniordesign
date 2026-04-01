import os
import time
from dotenv import load_dotenv
import pyrebase

load_dotenv()

firebase_config = {
    "apiKey": os.getenv("FIREBASE_API_KEY"),
    "authDomain": "hq-users.firebaseapp.com",
    "projectId": "hq-users",
    "databaseURL": "https://hq-users-default-rtdb.firebaseio.com",
    "storageBucket": "hq-users.firebasestorage.app"
}

firebase = pyrebase.initialize_app(firebase_config)
auth = firebase.auth()
db = firebase.database()

def sign_up(email: str, password: str):
    try:
        user = auth.create_user_with_email_and_password(email, password)
        return {"success": True, "uid": user['localId'], "token": user['idToken']}
    except Exception as e:
        return {"success": False, "error": str(e)}

def log_in(email: str, password: str):
    try:
        user = auth.sign_in_with_email_and_password(email, password)
        return {"success": True, "uid": user['localId'], "token": user['idToken']}
    except Exception as e:
        return {"success": False, "error": str(e)}


def verify_token(id_token: str):
    try:
        info = auth.get_account_info(id_token)
        users = info.get("users", [])
        if not users:
            return {"success": False, "error": "Invalid token"}
        user = users[0]
        return {
            "success": True,
            "uid": user.get("localId"),
            "email": user.get("email")
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def save_history_message(uid: str, conversation_id: str, role: str, content: str):
    try:
        payload = {
            "role": role,
            "content": content,
            "createdAt": int(time.time() * 1000),
        }
        db.child("histories").child(uid).child(conversation_id).child("messages").push(payload)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_conversation_messages(uid: str, conversation_id: str):
    try:
        data = (
            db.child("histories")
            .child(uid)
            .child(conversation_id)
            .child("messages")
            .get()
            .val()
        )
        if not data:
            return {"success": True, "messages": []}

        messages = []
        for item in data.values():
            if not isinstance(item, dict):
                continue
            messages.append(
                {
                    "role": item.get("role", "assistant"),
                    "content": item.get("content", ""),
                    "createdAt": item.get("createdAt", 0),
                }
            )
        messages.sort(key=lambda msg: msg.get("createdAt", 0))
        return {"success": True, "messages": messages}
    except Exception as e:
        return {"success": False, "error": str(e)}


def list_conversations(uid: str):
    try:
        data = db.child("histories").child(uid).get().val()
        if not data:
            return {"success": True, "conversations": []}

        conversations = []
        for conversation_id, conversation_data in data.items():
            latest = 0
            if isinstance(conversation_data, dict):
                messages = conversation_data.get("messages", {})
                if isinstance(messages, dict):
                    for item in messages.values():
                        if isinstance(item, dict):
                            latest = max(latest, item.get("createdAt", 0) or 0)
            conversations.append(
                {
                    "conversationId": conversation_id,
                    "latestMessageAt": latest,
                }
            )

        conversations.sort(key=lambda conv: conv.get("latestMessageAt", 0), reverse=True)
        return {"success": True, "conversations": conversations}
    except Exception as e:
        return {"success": False, "error": str(e)}