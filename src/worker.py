from js import Response
import json


async def on_fetch(request, env):
    """Handle HTTP requests"""
    return Response.new("Hello from six_worker Python Worker!")


async def queue_handler(batch, env):
    """Process queue messages"""
    print(f"Processing {len(batch.messages)} messages from queue")
    
    for message in batch.messages:
        try:
            # Process each message
            print(f"Processing message: {message.body}")
            # TODO: Add database operations using Hyperdrive
            
            # Acknowledge successful processing
            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")
            # Retry the message
            message.retry()


# Export the handlers
export = {
    "default": {
        "fetch": on_fetch,
        "queue": queue_handler
    }
}