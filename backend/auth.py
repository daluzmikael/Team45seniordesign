import pyrebase

firebase_config = {
    "apiKey": "AIzaSyAWAsFoESLPzgj9NNUMdXA3L6xr2LtK62s",
    "authDomain": "hq-users.firebaseapp.com",
    "projectId": "hq-users",
    "databaseURL": "",
    "storageBucket": "hq-users.firebasestorage.app"
}

firebase = pyrebase.initialize_app(firebase_config)
auth = firebase.auth()

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