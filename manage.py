"""Server-side account tools (run on the VPS, in the forex-bot folder).

    python manage.py create-founder --email you@example.com --name "Your Name"
    python manage.py reset-password --email someone@example.com
    python manage.py set-role --email someone@example.com --role admin
    python manage.py extend --email someone@example.com --days 30
    python manage.py unban --email someone@example.com
    python manage.py list-users
"""
import argparse
import getpass
import sys
from datetime import timedelta

from sqlalchemy import select

from server.auth import hash_password, temp_password, validate_password
from server.db import SessionLocal, User, audit, init_db, utcnow
from server.settings import DEFAULT_USER_SETTINGS


def find(db, email: str) -> User:
    u = db.scalar(select(User).where(User.email == email.strip().lower()))
    if not u:
        sys.exit(f"No user with email {email}")
    return u


def ask_password() -> str:
    pw = getpass.getpass("Password (min 8 characters): ")
    if pw != getpass.getpass("Repeat password: "):
        sys.exit("Passwords do not match.")
    try:
        validate_password(pw)
    except Exception:
        sys.exit("Password must be 8-200 characters.")
    return pw


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("create-founder"); p.add_argument("--email", required=True); p.add_argument("--name", default="")
    p = sub.add_parser("reset-password"); p.add_argument("--email", required=True)
    p = sub.add_parser("set-role"); p.add_argument("--email", required=True); p.add_argument("--role", choices=["admin", "client"], required=True)
    p = sub.add_parser("extend"); p.add_argument("--email", required=True); p.add_argument("--days", type=int, required=True)
    p = sub.add_parser("unban"); p.add_argument("--email", required=True)
    sub.add_parser("list-users")
    args = ap.parse_args()

    init_db()
    with SessionLocal() as db:
        if args.cmd == "create-founder":
            if db.scalar(select(User).where(User.role == "founder")):
                sys.exit("A founder account already exists. Only the founder can delete it (in the web app).")
            email = args.email.strip().lower()
            u = db.scalar(select(User).where(User.email == email))
            pw = ask_password()
            if u:
                u.role, u.password_hash, u.token_version = "founder", hash_password(pw), u.token_version + 1
            else:
                u = User(email=email, name=args.name, password_hash=hash_password(pw), role="founder", plan="founder",
                         settings=dict(DEFAULT_USER_SETTINGS))
                db.add(u)
            db.commit()
            audit(db, None, "founder_created_cli", u)
            print(f"Founder account ready: {email}. Restart the server to clear the setup token.")
        elif args.cmd == "reset-password":
            u = find(db, args.email)
            pw = temp_password()
            u.password_hash, u.token_version = hash_password(pw), u.token_version + 1
            db.commit()
            audit(db, None, "password_reset_cli", u)
            print(f"Temporary password for {u.email}: {pw}")
        elif args.cmd == "set-role":
            u = find(db, args.email)
            if u.role == "founder":
                sys.exit("The founder's role cannot be changed here.")
            u.role, u.token_version = args.role, u.token_version + 1
            db.commit()
            audit(db, None, "role_cli", u, args.role)
            print(f"{u.email} is now {args.role}.")
        elif args.cmd == "extend":
            u = find(db, args.email)
            u.subscription_until = max(u.subscription_until or utcnow(), utcnow()) + timedelta(days=args.days)
            db.commit()
            audit(db, None, "extend_cli", u, f"{args.days} days")
            print(f"{u.email} active until {u.subscription_until:%Y-%m-%d %H:%M} UTC.")
        elif args.cmd == "unban":
            u = find(db, args.email)
            u.is_banned, u.ban_reason = False, ""
            db.commit()
            audit(db, None, "unban_cli", u)
            print(f"{u.email} unbanned.")
        elif args.cmd == "list-users":
            for u in db.scalars(select(User).order_by(User.created_at)):
                state = "BANNED" if u.is_banned else ("active" if u.subscription_active else "expired")
                until = f"{u.subscription_until:%Y-%m-%d}" if u.subscription_until else "-"
                print(f"{u.id:>4}  {u.role:<8} {state:<8} {until:<11} {u.email}")


if __name__ == "__main__":
    main()
