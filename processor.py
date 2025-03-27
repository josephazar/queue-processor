import os
import json
import time
import logging
import asyncio
import traceback
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/processor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("nl2sql_processor")

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)



# Configuration from environment variables
CONNECTION_STRING = os.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING")
QUEUE_NAME = os.getenv("AZURE_SERVICE_BUS_QUEUE_NAME", "nl2sql-requests")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
MAX_MESSAGE_COUNT = int(os.getenv("MAX_MESSAGE_COUNT", "10"))
MAX_WAIT_TIME = int(os.getenv("MAX_WAIT_TIME", "5"))
CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "7"))
CLEANUP_INTERVAL_HOURS = int(os.getenv("CLEANUP_INTERVAL_HOURS", "24"))


async def process_message(message, receiver):
    """
    Process a single message from the queue
    """
    try:
        # Convert message body to bytes if it's a generator
        if isinstance(message.body, (bytes, str)):
            body_bytes = message.body
        else:
            body_bytes = b"".join(message.body)  # Handle generator case

        # Decode and parse the message body
        body_str = body_bytes.decode('utf-8') if isinstance(body_bytes, bytes) else body_bytes
        body = json.loads(body_str)
        
        request_id = body.get("request_id")
        question = body.get("question")
        assistant_id = body.get("assistant_id")
        thread_id = body.get("thread_id")
        user_email = body.get("user_email")  # Get user email from message
        
        logger.info(f"Processing request {request_id} for user {user_email} with question: {question[:50]}...")
        
        # Complete the message to remove it from the queue using receiver
        receiver.complete_message(message)
        
        logger.info(f"Completed request {request_id}")
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        logger.error(traceback.format_exc())
        # In a real scenario, you might want to implement retry logic
        # or move the message to a dead-letter queue
        receiver.abandon_message(message)  # Return to queue for retry

def run_async_in_thread(async_func, *args):
    """
    Run an async function in a separate thread using a new event loop
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(async_func(*args))
    finally:
        loop.close()

def process_message_in_thread(message, receiver):
    """
    Wrapper to process message in a thread
    """
    return run_async_in_thread(process_message, message, receiver)


def main():
    """
    Main function to process messages from the queue
    """
    if not CONNECTION_STRING:
        logger.error("Azure Service Bus connection string is not set!")
        return
    
    logger.info(f"Starting message processing with {MAX_WORKERS} workers")
    logger.info(f"Connecting to queue: {QUEUE_NAME}")
    

    
    # Create ServiceBusClient
    servicebus_client = ServiceBusClient.from_connection_string(CONNECTION_STRING)
    
    # Create a receiver in peek-lock mode (default) so we can complete/abandon messages
    receiver = servicebus_client.get_queue_receiver(
        queue_name=QUEUE_NAME,
        max_wait_time=MAX_WAIT_TIME
    )
    
    # Create thread pool for parallel processing
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        try:
            logger.info("Starting message processing loop")
            while True:
                # Receive batch of messages
                messages = receiver.receive_messages(
                    max_message_count=MAX_MESSAGE_COUNT,
                    max_wait_time=MAX_WAIT_TIME
                )
                
                if messages:
                    logger.info(f"Received {len(messages)} messages")
                    # Submit each message to the thread pool
                    for message in messages:
                        executor.submit(process_message_in_thread, message, receiver)
                else:
                    # No messages, sleep briefly before polling again
                    time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping message processing due to keyboard interrupt")
        except Exception as e:
            logger.error(f"Error in message processing loop: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            logger.info("Closing receiver and client connections")
            receiver.close()
            servicebus_client.close()

if __name__ == "__main__":
    logger.info("NL2SQL Queue Processor starting up")
    main()