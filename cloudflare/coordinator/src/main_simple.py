"""
Simple test worker
"""
from js import Response

async def on_fetch(request, env):
    """Main request handler"""
    return Response.new('{"status":"ok"}', status=200)

export_default = {
    'fetch': on_fetch
}
