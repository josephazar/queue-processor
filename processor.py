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
import importlib.util

# Add src directory to the path for importing
base_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(base_dir, "src")
sys.path.insert(0, src_dir)

# Import our custom modules
from config import (
    AZURE_SERVICE_BUS_CONNECTION_STRING,
    AZURE_SERVICE_BUS_QUEUE_NAME,
    MAX_WORKERS,
    MAX_MESSAGE_COUNT,
    MAX_WAIT_TIME,
    CLEANUP_DAYS,
    CLEANUP_INTERVAL_HOURS,
    LOGS_DIR,
    DATABASE_TYPE,
    validate_config
)
from database import store_request, update_request_status, cleanup_old_requests, get_request_status

# Check if src/main.py exists and import initialize_assistant
main_path = os.path.join(src_dir, "main.py")
if os.path.exists(main_path):
    spec = importlib.util.spec_from_file_location("main", main_path)
    main_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(main_module)
    initialize_assistant = main_module.initialize_assistant
else:
    raise ImportError("Could not find src/main.py which contains initialize_assistant")

# Configure logging
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "processor.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("nl2sql_processor")

# Validate configuration
validate_config()

# Last cleanup time tracking
last_cleanup_time = 0

# Thread safety mechanisms
active_requests = {}
active_requests_lock = threading.RLock()

# Cache for assistants to avoid recreating them
assistant_cache = {}
assistant_cache_lock = threading.RLock()

async def delete_assistant(assistant_id):
    """
    Delete an assistant by ID
    This implementation mirrors the one in nl2sqlroute.py
    """
    try:
        if not assistant_id:
            return {"status": "error", "message": "No assistant ID provided"}
        
        # Construct the API URL from environment variables
        endpoint = os.getenv("AZURE_OPENAI_API_ENDPOINT")
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        url = f"{endpoint}/openai/assistants/{assistant_id}?api-version={api_version}"

        # Import requests here to avoid global import
        import requests
        
        # Make the DELETE request
        response = requests.delete(url, headers={"api-key": os.getenv("AZURE_OPENAI_API_KEY")})

        # Handle the response
        if response.status_code in [200, 201, 202, 204]:
            logger.info(f"Assistant {assistant_id} deleted successfully")
            
            # Remove from cache if present
            with assistant_cache_lock:
                for user_email, info in list(assistant_cache.items()):
                    if info.get("assistant_id") == assistant_id:
                        del assistant_cache[user_email]
                        break
            
            return {
                "status": "success", 
                "message": f"Assistant {assistant_id} deleted successfully",
                "assistant_id": None,
                "thread_id": None
            }
        else:
            error_msg = f"Error deleting assistant: {response.text}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "assistant_id": assistant_id,
                "thread_id": None
            }
    except Exception as e:
        logger.error(f"Error deleting assistant: {e}")
        traceback.print_exc()
        return {
            "status": "error", 
            "message": f"Error deleting assistant: {str(e)}",
            "assistant_id": assistant_id,
            "thread_id": None
        }

def get_cached_assistant(user_email):
    """Get assistant from cache if it exists"""
    with assistant_cache_lock:
        if user_email in assistant_cache:
            info = assistant_cache[user_email]
            # Update last used time
            assistant_cache[user_email]["last_used"] = time.time()
            return info.get("assistant_id"), info.get("thread_id")
    return None, None

def cache_assistant(user_email, assistant_id, thread_id=None):
    """Store assistant in cache"""
    with assistant_cache_lock:
        assistant_cache[user_email] = {
            "assistant_id": assistant_id,
            "thread_id": thread_id,
            "last_used": time.time()
        }

async def process_question(request_id, question, assistant_id=None, thread_id=None, user_email=None):
    """
    Process a user question using the NL2SQL assistant.
    This is the core function that communicates with the AI model.
    
    Args:
        request_id (str): Unique identifier for the request
        question (str): User's natural language question
        assistant_id (str, optional): ID of an existing assistant to use
        thread_id (str, optional): ID of an existing conversation thread
        user_email (str, optional): Email of the user making the request
        
    Returns:
        dict: Response with assistant_id, thread_id, and the answer
    """
    try:
        logger.info(f"Processing question for request {request_id}: '{question[:50]}...'")
        
        # Check if this is a termination request
        if any(word == question.lower() for word in ["bye", "exit", "end"]):
            if assistant_id:
                # If we have an assistant_id, delete it using the delete_assistant function
                return await delete_assistant(assistant_id)
            else:
                return {
                    "status": "success", 
                    "message": "Goodbye!", 
                    "assistant_id": None, 
                    "thread_id": None
                }
        
        # Check cache for existing assistant
        if not assistant_id and user_email:
            cached_assistant_id, cached_thread_id = get_cached_assistant(user_email)
            if cached_assistant_id:
                logger.info(f"Using cached assistant {cached_assistant_id} for user {user_email}")
                assistant_id = cached_assistant_id
                # Only use cached thread if explicitly provided or not processing a new question
                if thread_id is None and cached_thread_id:
                    thread_id = cached_thread_id
        
        # Initialize assistant - either new or existing
        if assistant_id:
            logger.info(f"Using assistant: {assistant_id} (new: False)")
            sql_assistant = initialize_assistant(DATABASE_TYPE, assistant_id=assistant_id)
        else:
            logger.info(f"Creating new assistant for user {user_email}")
            sql_assistant = initialize_assistant(DATABASE_TYPE)
            assistant_id = sql_assistant.assistant.assistant_id
            logger.info(f"Using assistant: {assistant_id} (new: True)")
            
        # Create a thread if needed
        if not thread_id:
            thread = sql_assistant.assistant.create_thread()
            thread_id = thread.id
            logger.info(f"Created new thread {thread_id}")
        
        # Update cache with assistant info
        if user_email:
            cache_assistant(user_email, assistant_id, thread_id)
        
        # Process the question with a timeout
        try:
            # Use a timer to implement a timeout since we can't use asyncio.wait_for
            start_time = time.time()
            timeout = 120  # 2 minute timeout
            
            # Call create_response directly - it's not awaitable
            response_dict = sql_assistant.assistant.create_response(
                question=question,
                thread_id=thread_id
            )
            
            # Check if we timed out during processing
            if time.time() - start_time > timeout:
                logger.warning(f"Request {request_id} processing took longer than {timeout} seconds")
            
            # Extract answer
            answer = response_dict.get("answer", "No answer was generated")
            
            # Return the response with assistant and thread IDs
            return {
                "status": "success", 
                "response": answer,
                "assistant_id": assistant_id,
                "thread_id": thread_id
            }
        except Exception as e:
            logger.error(f"Error calling create_response: {str(e)}")
            return {
                "status": "error",
                "message": f"Error processing question: {str(e)}",
                "assistant_id": assistant_id,
                "thread_id": thread_id
            }
    
    except Exception as e:
        logger.error(f"Error processing question for request {request_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error processing question: {str(e)}",
            "assistant_id": assistant_id,
            "thread_id": thread_id
        }

async def process_message(message, receiver):
    """
    Process a single message from the queue with improved thread safety
    """
    request_id = None
    processing_started = False
    
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
        user_email = body.get("user_email")
        request_type = body.get("request_type", "nl2sql_chat")
        
        if not request_id or not question:
            logger.error("Message missing required fields (request_id, question)")
            receiver.complete_message(message)
            return
        
        # Thread safety: Check if this request is already being processed
        with active_requests_lock:
            if request_id in active_requests:
                logger.info(f"Request {request_id} is already being processed, skipping")
                receiver.complete_message(message)
                return
            
            # Mark this request as being processed
            active_requests[request_id] = time.time()
            processing_started = True
        
        # Check the current status of the request
        current_status = get_request_status(request_id)
        if current_status and current_status.get("status") in ["completed", "error"]:
            logger.info(f"Request {request_id} is already {current_status.get('status')}, skipping")
            receiver.complete_message(message)
            
            # Remove from active requests
            with active_requests_lock:
                if request_id in active_requests:
                    del active_requests[request_id]
            return
        
        logger.info(f"Processing request {request_id} for user {user_email}")
        
        # Update the request status to processing
        update_request_status(request_id, "processing")
        
        # Process the question
        result = await process_question(
            request_id=request_id,
            question=question,
            assistant_id=assistant_id,
            thread_id=thread_id,
            user_email=user_email
        )
        
        # Update the status based on the result
        status = "completed" if result.get("status") == "success" else "error"
        update_request_status(request_id, status, result)
        
        # Complete the message to remove it from the queue
        receiver.complete_message(message)
        
        logger.info(f"Completed processing request {request_id}")
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Update status if we have a request_id
        if request_id:
            error_result = {
                "status": "error",
                "error": str(e),
                "stacktrace": traceback.format_exc()
            }
            update_request_status(request_id, "error", error_result)
        
        # Return to queue for retry if not already being processed
        receiver.abandon_message(message)
    
    finally:
        # Always clean up the active requests tracker
        if processing_started and request_id:
            with active_requests_lock:
                if request_id in active_requests:
                    del active_requests[request_id]

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

def cleanup_task():
    """
    Periodically clean up old requests
    """
    global last_cleanup_time
    
    current_time = time.time()
    time_since_last_cleanup = current_time - last_cleanup_time
    
    # Convert hours to seconds for comparison
    cleanup_interval_seconds = CLEANUP_INTERVAL_HOURS * 3600
    
    if time_since_last_cleanup >= cleanup_interval_seconds:
        logger.info(f"Running cleanup task for requests older than {CLEANUP_DAYS} days")
        try:
            deleted_items = cleanup_old_requests(CLEANUP_DAYS)
            logger.info(f"Cleaned up {len(deleted_items)} old requests")
            last_cleanup_time = current_time
            
            # Also clean up old assistant cache entries
            with assistant_cache_lock:
                current_time = time.time()
                expired_keys = []
                for user_email, info in assistant_cache.items():
                    if current_time - info.get("last_used", 0) > 3600:  # 1 hour expiry
                        expired_keys.append(user_email)
                
                for key in expired_keys:
                    del assistant_cache[key]
                
                if expired_keys:
                    logger.info(f"Cleaned up {len(expired_keys)} expired assistant cache entries")
                
        except Exception as e:
            logger.error(f"Error during cleanup task: {str(e)}")

def main():
    """
    Main function to process messages from the queue
    """
    global last_cleanup_time
    
    if not AZURE_SERVICE_BUS_CONNECTION_STRING:
        logger.error("Azure Service Bus connection string is not set!")
        return
    
    logger.info(f"Starting message processing with {MAX_WORKERS} workers")
    logger.info(f"Connecting to queue: {AZURE_SERVICE_BUS_QUEUE_NAME}")
    
    # Initialize last_cleanup_time
    last_cleanup_time = time.time()
    
    # Create ServiceBusClient
    servicebus_client = ServiceBusClient.from_connection_string(AZURE_SERVICE_BUS_CONNECTION_STRING)
    
    # Create a receiver in peek-lock mode (default) so we can complete/abandon messages
    receiver = servicebus_client.get_queue_receiver(
        queue_name=AZURE_SERVICE_BUS_QUEUE_NAME,
        max_wait_time=MAX_WAIT_TIME
    )
    
    # Create thread pool for parallel processing
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        try:
            logger.info("Starting message processing loop")
            while True:
                # Run cleanup task
                cleanup_task()
                
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