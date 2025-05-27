from starlette.middleware.base import BaseHTTPMiddleware
import uuid
from fastapi import Request

user_sessions = {}


class SessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        session_id = request.cookies.get("session_id")
        if session_id in user_sessions:
            request.state.session = user_sessions[session_id]
        elif session_id is None:
            new_session_id = str(uuid.uuid4())
            request.state.session = {}
            user_sessions[new_session_id] = request.state.session
            session_id = new_session_id
        else:
            request.state.session = {}
            user_sessions[session_id] = request.state.session

        response = await call_next(request)

        if request.cookies.get("session_id") is None:
            cookie_life = 15 * 60
            response.set_cookie(
                key="session_id",
                value=session_id,
                httponly=True,
                max_age=cookie_life,
            )

        return response

