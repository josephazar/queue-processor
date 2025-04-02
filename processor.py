import os
import json
import time
import asyncio
import traceback
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiveMode
from azure.servicebus.exceptions import ServiceBusConnectionError, ServiceBusError
import importlib.util
from pathlib import Path

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

# Import database functions with retry capabilities
from database import (
    store_request, 
    update_request_status, 
    cleanup_old_requests, 
    get_request_status,
    store_conversation,
    get_assistant_last_activity,
    cleanup_old_conversations,
    bulk_store_conversations,
    log_container_health_issue
)

# Import logging utils
from logging_utils import setup_logging

# Check if src/main.py exists and import initialize_assistant
main_path = os.path.join(src_dir, "main.py")
if os.path.exists(main_path):
    spec = importlib.util.spec_from_file_location("main", main_path)
    main_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(main_module)
    initialize_assistant = main_module.initialize_assistant
else:
    raise ImportError("Could not find src/main.py which contains initialize_assistant")

# Set up logging with environment-aware configuration
logger = setup_logging(LOGS_DIR, "nl2sql_processor")
logger.info("NL2SQL Queue Processor starting up")

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

# Message batch tracking for bulk operations
pending_conversations = []
pending_conversations_lock = threading.RLock()

# Health monitoring variables
last_health_check = 0
last_message_received = 0
consecutive_connection_errors = 0

# Constants
ASSISTANT_INACTIVITY_TIMEOUT = 60 * 60  # 60 minutes in seconds
BULK_INSERT_THRESHOLD = 10  # Number of conversations to batch before inserting
HEALTH_CHECK_INTERVAL = 300  # 5 minutes
NO_MESSAGES_TIMEOUT = 1800  # 30 minutes (no messages received)
MAX_CONNECTION_ERRORS = 10  # Max consecutive connection errors before restart
CONNECTION_ERROR_SLEEP = 10  # Seconds to sleep after a connection error

def check_container_health():
    """
    Check the health of the container by testing connections to critical services.
    
    Returns:
        bool: True if all health checks pass, False otherwise
    """
    global consecutive_connection_errors
    
    try:
        logger.info("Performing container health check")
        
        # Check Service Bus connectivity
        try:
            servicebus_client = ServiceBusClient.from_connection_string(
                AZURE_SERVICE_BUS_CONNECTION_STRING,
                retry_total=3
            )
            with servicebus_client:
                receiver = servicebus_client.get_queue_receiver(
                    queue_name=AZURE_SERVICE_BUS_QUEUE_NAME,
                    max_wait_time=5  # Short timeout for health check
                )
                with receiver:
                    # Just peek a message to verify connectivity
                    receiver.peek_messages(max_message_count=1)
            logger.info("Service Bus health check: OK")
        except (ServiceBusConnectionError, ServiceBusError) as e:
            logger.error(f"Service Bus health check failed: {str(e)}")
            log_container_health_issue("servicebus_connectivity", str(e))
            consecutive_connection_errors += 1
            return False
            
        # Check Cosmos DB connectivity
        try:
            # Test connection by making a simple query
            from database import CosmosDBManager
            CosmosDBManager.get_instance().get_collection().find_one({}, limit=1)
            logger.info("Cosmos DB health check: OK")
        except Exception as e:
            logger.error(f"Cosmos DB health check failed: {str(e)}")
            log_container_health_issue("cosmosdb_connectivity", str(e))
            consecutive_connection_errors += 1
            return False
            
        # Check thread pool health
        with active_requests_lock:
            current_time = time.time()
            stuck_requests = {}
            for request_id, start_time in active_requests.items():
                if current_time - start_time > 600:  # 10 minutes is too long
                    stuck_requests[request_id] = int(current_time - start_time)
                    
            if stuck_requests:
                logger.warning(f"Found {len(stuck_requests)} potentially stuck requests: {stuck_requests}")
                log_container_health_issue("stuck_threads", json.dumps(stuck_requests))
                # Don't fail health check for this, but log it
        
        # All checks passed
        consecutive_connection_errors = 0
        logger.info("All container health checks passed")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error during health check: {str(e)}", exc_info=True)
        log_container_health_issue("health_check_error", str(e))
        consecutive_connection_errors += 1
        return False

def restart_processing():
    """
    Log that we're restarting and exit with a code that signals container to restart
    """
    logger.critical("Restarting processing due to detected issues")
    log_container_health_issue("container_restart", "Initiated restart due to detected issues")
    
    # Flush any remaining conversations
    if len(pending_conversations) > 0:
        maybe_flush_conversation_batch(force=True)
        
    # Close Cosmos DB connection
    try:
        from database import CosmosDBManager
        CosmosDBManager.get_instance().close()
    except Exception as e:
        logger.error(f"Error closing Cosmos DB connection: {str(e)}")
    
    # Exit with code 1 to signal container needs to restart
    sys.exit(1)

def monitor_thread_health(executor):
    """
    Monitor thread health and log if threads appear stuck
    """
    with active_requests_lock:
        current_time = time.time()
        stuck_requests = {}
        for request_id, start_time in active_requests.items():
            if current_time - start_time > 300:  # 5 minutes
                stuck_requests[request_id] = int(current_time - start_time)
        
        if stuck_requests:
            logger.warning(f"Found {len(stuck_requests)} potentially stuck requests: {stuck_requests}")
            # Log to Cosmos DB
            log_container_health_issue("stuck_threads", json.dumps(stuck_requests))

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
            logger.log_with_context(
                f"Assistant deleted successfully", 
                assistant_id=assistant_id
            )
            
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
            logger.error_with_context(
                error_msg,
                assistant_id=assistant_id
            )
            return {
                "status": "error",
                "message": error_msg,
                "assistant_id": assistant_id,
                "thread_id": None
            }
    except Exception as e:
        logger.error_with_context(
            f"Error deleting assistant", 
            assistant_id=assistant_id,
            exc_info=True,
            error=str(e)
        )
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

def maybe_flush_conversation_batch(force=True):
    """Check if we have enough pending conversations to do a bulk insert"""
    global pending_conversations
    conversations_to_insert = []
    with pending_conversations_lock:
        if force or len(pending_conversations) >= BULK_INSERT_THRESHOLD:
            conversations_to_insert = pending_conversations[:]
            pending_conversations = []
    
    if conversations_to_insert:
        try:
            count = bulk_store_conversations(conversations_to_insert)
            logger.log_with_context(f"Bulk inserted {count} conversations")
        except Exception as e:
            logger.error_with_context(
                "Failed to bulk insert conversations", 
                exc_info=True,
                count=len(conversations_to_insert),
                error=str(e)
            )
            # Fall back to individual inserts
            for conv in conversations_to_insert:
                try:
                    store_conversation(
                        request_id=conv["request_id"],
                        question=conv["question"],
                        answer=conv["answer"],
                        user_email=conv.get("user_email"),
                        assistant_id=conv.get("assistant_id"),
                        thread_id=conv.get("thread_id"),
                        report_name=conv.get("report_name"),
                        request_type=conv.get("request_type"),
                        conversation_id=conv.get("conversation_id")
                    )
                except Exception as inner_e:
                    logger.error_with_context(
                        "Failed to store individual conversation", 
                        request_id=conv.get("request_id"),
                        error=str(inner_e)
                    )

async def process_question(request_id, question, assistant_id=None, thread_id=None, user_email=None, request_type=None, report_name=None):
    """
    Process a user question using the NL2SQL assistant.
    This is the core function that communicates with the AI model.
    """
    start_time = time.time()
    assistant_created = False  # Track if we created a new assistant
    
    try:
        logger.log_with_context(
            f"Processing question", 
            request_id=request_id,
            user_email=user_email,
            assistant_id=assistant_id,
            thread_id=thread_id
        )
        
        # Check if this is a termination request
        if any(word == question.lower() for word in ["bye", "exit", "end"]):
            logger.log_with_context(
                "Received termination request", 
                request_id=request_id, 
                user_email=user_email
            )
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
                logger.log_with_context(
                    "Using cached assistant", 
                    request_id=request_id,
                    user_email=user_email,
                    assistant_id=cached_assistant_id
                )
                assistant_id = cached_assistant_id
                # Only use cached thread if explicitly provided or not processing a new question
                if thread_id is None and cached_thread_id:
                    thread_id = cached_thread_id
        
        # Initialize assistant - either new or existing
        if assistant_id:
            logger.log_with_context(
                "Using existing assistant", 
                request_id=request_id,
                assistant_id=assistant_id
            )
            sql_assistant = initialize_assistant(DATABASE_TYPE, assistant_id=assistant_id)
        else:
            logger.log_with_context(
                "Creating new assistant", 
                request_id=request_id,
                user_email=user_email
            )
            sql_assistant = initialize_assistant(DATABASE_TYPE)
            assistant_id = sql_assistant.assistant.assistant_id
            assistant_created = True  # Mark that we created a new assistant
            logger.log_with_context(
                "Created new assistant", 
                request_id=request_id,
                assistant_id=assistant_id
            )
            
        # Create a thread if needed
        if not thread_id:
            thread = sql_assistant.assistant.create_thread()
            thread_id = thread.id
            logger.log_with_context(
                "Created new thread", 
                request_id=request_id,
                thread_id=thread_id,
                assistant_id=assistant_id
            )
        
        # Only update cache if everything is successful so far
        # We'll update the cache at the end if all processing succeeds
        
        # Process the question with a timeout
        try:
            # Use a timer to implement a timeout since we can't use asyncio.wait_for
            start_processing_time = time.time()
            timeout = 120  # 2 minute timeout
            
            # Call create_response directly - it's not awaitable
            response_dict = sql_assistant.assistant.create_response(
                question=question,
                thread_id=thread_id
            )
            
            # Check if we timed out during processing
            processing_duration = time.time() - start_processing_time
            if processing_duration > timeout:
                logger.warning(
                    f"Request processing took longer than timeout", 
                    request_id=request_id,
                    duration=processing_duration,
                    timeout=timeout
                )
            
            # Extract answer
            answer = response_dict.get("answer", "No answer was generated")
            
            # Prepare conversation document
            conversation_doc = {
                "request_id": request_id,
                "question": question,
                "answer": answer,
                "user_email": user_email,
                "assistant_id": assistant_id,
                "thread_id": thread_id,
                "report_name": report_name,
                "request_type": request_type,
                "conversation_id": None,  # We don't have conversation_id yet
                "created_at": int(time.time()),
                "updated_at": int(time.time())
            }
            
            # Add to batch for bulk insertion
            with pending_conversations_lock:
                pending_conversations.append(conversation_doc)
            
            # Check if we should do a bulk insert
            maybe_flush_conversation_batch()
            
            # If we've gotten this far, it's safe to update the cache
            if user_email:
                cache_assistant(user_email, assistant_id, thread_id)
            
            # Calculate total processing time
            total_duration = time.time() - start_time
            logger.log_with_context(
                "Completed processing question", 
                request_id=request_id,
                user_email=user_email,
                duration=total_duration
            )
            
            # Return the response with assistant and thread IDs
            return {
                "status": "success", 
                "response": answer,
                "assistant_id": assistant_id,
                "thread_id": thread_id
            }
        except Exception as e:
            logger.error_with_context(
                "Error calling create_response", 
                request_id=request_id,
                assistant_id=assistant_id,
                thread_id=thread_id,
                exc_info=True,
                error=str(e)
            )
            
            # If we created a new assistant and there was an error, clean it up
            if assistant_created:
                try:
                    logger.log_with_context(
                        "Cleaning up assistant after error", 
                        assistant_id=assistant_id
                    )
                    await delete_assistant(assistant_id)
                except Exception as cleanup_error:
                    logger.error_with_context(
                        "Failed to clean up assistant after error", 
                        assistant_id=assistant_id,
                        error=str(cleanup_error)
                    )
            
            return {
                "status": "error",
                "message": f"Error processing question: {str(e)}",
                "assistant_id": None if assistant_created else assistant_id,
                "thread_id": thread_id
            }
    
    except Exception as e:
        logger.error_with_context(
            "Error processing question", 
            request_id=request_id,
            user_email=user_email,
            exc_info=True,
            error=str(e)
        )
        
        # If we created a new assistant and there was an error, clean it up
        if assistant_created:
            try:
                logger.log_with_context(
                    "Cleaning up assistant after error", 
                    assistant_id=assistant_id
                )
                await delete_assistant(assistant_id)
            except Exception as cleanup_error:
                logger.error_with_context(
                    "Failed to clean up assistant after error", 
                    assistant_id=assistant_id,
                    error=str(cleanup_error)
                )
        
        return {
            "status": "error",
            "message": f"Error processing question: {str(e)}",
            "assistant_id": None if assistant_created else assistant_id,
            "thread_id": thread_id
        }
    
async def process_message(message, receiver):
    """
    Process a single message from the queue with improved thread safety
    """
    request_id = None
    processing_started = False
    start_time = time.time()
    
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
        report_name = body.get("report_name")  # Add report_name extraction
        
        if not request_id or not question:
            logger.error_with_context(
                "Message missing required fields", 
                request_id=request_id,
                fields=body.keys()
            )
            receiver.complete_message(message)
            return
        
        # Thread safety: Check if this request is already being processed
        with active_requests_lock:
            if request_id in active_requests:
                logger.log_with_context(
                    "Request already being processed, skipping", 
                    request_id=request_id,
                    user_email=user_email
                )
                receiver.complete_message(message)
                return
            
            # Mark this request as being processed
            active_requests[request_id] = time.time()
            processing_started = True
        
        # Check the current status of the request
        current_status = get_request_status(request_id)
        if current_status and current_status.get("status") in ["completed", "error"]:
            logger.log_with_context(
                "Request already processed, skipping", 
                request_id=request_id,
                status=current_status.get("status")
            )
            receiver.complete_message(message)
            
            # Remove from active requests
            with active_requests_lock:
                if request_id in active_requests:
                    del active_requests[request_id]
            return
        
        logger.log_with_context(
            "Processing request", 
            request_id=request_id,
            user_email=user_email,
            request_type=request_type
        )
        
        # Update the request status to processing
        update_request_status(request_id, "processing")
        
        # Process the question
        result = await process_question(
            request_id=request_id,
            question=question,
            assistant_id=assistant_id,
            thread_id=thread_id,
            user_email=user_email,
            request_type=request_type,
            report_name=report_name
        )
        
        # Update the status based on the result
        status = "completed" if result.get("status") == "success" else "error"
        update_request_status(request_id, status, result)
        
        # Complete the message to remove it from the queue
        receiver.complete_message(message)
        
        # Calculate total processing time
        total_duration = time.time() - start_time
        logger.log_with_context(
            "Completed processing request", 
            request_id=request_id,
            user_email=user_email,
            duration=total_duration,
            status=status
        )
        
    except Exception as e:
        logger.error_with_context(
            "Error processing message", 
            request_id=request_id,
            user_email=user_email if 'user_email' in locals() else None,
            exc_info=True,
            error=str(e)
        )
        
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
                    
        # Force flush any pending conversations regardless of count
        maybe_flush_conversation_batch(force=True)

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

def process_message_in_thread(message, action_queue):
    """
    Wrapper to process message in a thread
    """
    return run_async_in_thread(process_message, message, action_queue)

async def check_assistant_inactivity(assistant_id):
    """
    Check if an assistant has been inactive for more than the inactivity timeout
    
    Args:
        assistant_id (str): The assistant ID to check
        
    Returns:
        bool: True if the assistant is inactive and should be deleted, False otherwise
    """
    last_activity = get_assistant_last_activity(assistant_id)
    
    # If no activity found, use the cache's last_used time as fallback
    if last_activity is None:
        with assistant_cache_lock:
            for user_email, info in assistant_cache.items():
                if info.get("assistant_id") == assistant_id:
                    last_activity = info.get("last_used")
                    break
    
    if last_activity is None:
        # If still no activity info, assume it's inactive
        return True
    
    current_time = time.time()
    time_since_last_activity = current_time - last_activity
    
    return time_since_last_activity > ASSISTANT_INACTIVITY_TIMEOUT

def cleanup_task():
    """
    Periodically clean up old requests and inactive assistants
    """
    global last_cleanup_time
    
    current_time = time.time()
    time_since_last_cleanup = current_time - last_cleanup_time
    
    # Ensure all pending conversations are flushed first
    if len(pending_conversations) > 0:
        maybe_flush_conversation_batch()
    
    # Convert hours to seconds for comparison
    cleanup_interval_seconds = CLEANUP_INTERVAL_HOURS * 3600
    
    if time_since_last_cleanup >= cleanup_interval_seconds:
        logger.log_with_context(
            f"Running cleanup task",
            cleanup_days=CLEANUP_DAYS
        )
        try:
            # Clean up old requests
            deleted_items = cleanup_old_requests(CLEANUP_DAYS)
            logger.log_with_context(
                "Cleaned up old requests",
                count=len(deleted_items)
            )
            
            # Clean up old conversations (keep for 30 days by default)
            deleted_conversations = cleanup_old_conversations(30)
            logger.log_with_context(
                "Cleaned up old conversations",
                count=deleted_conversations
            )
            
            # Update last cleanup time
            last_cleanup_time = current_time
            
            # Clean up assistant cache entries and inactive assistants
            with assistant_cache_lock:
                # First, collect assistants that should be deleted
                assistants_to_delete = []
                expired_keys = []
                
                for user_email, info in assistant_cache.items():
                    assistant_id = info.get("assistant_id")
                    
                    # Check inactivity
                    is_inactive = run_async_in_thread(check_assistant_inactivity, assistant_id)
                    
                    if is_inactive:
                        logger.log_with_context(
                            "Assistant inactive, marking for deletion",
                            assistant_id=assistant_id,
                            user_email=user_email
                        )
                        assistants_to_delete.append(assistant_id)
                        expired_keys.append(user_email)
                
                # Now delete the assistants from Azure OpenAI
                for assistant_id in assistants_to_delete:
                    try:
                        logger.log_with_context(
                            "Deleting inactive assistant",
                            assistant_id=assistant_id
                        )
                        result = run_async_in_thread(delete_assistant, assistant_id)
                        if result.get("status") == "success":
                            logger.log_with_context(
                                "Successfully deleted assistant",
                                assistant_id=assistant_id
                            )
                        else:
                            logger.warning(
                                "Failed to delete assistant",
                                assistant_id=assistant_id,
                                error=result.get('message')
                            )
                    except Exception as e:
                        logger.error_with_context(
                            "Error deleting assistant",
                            assistant_id=assistant_id,
                            exc_info=True,
                            error=str(e)
                        )
                
                # Finally remove the expired entries from the cache
                for key in expired_keys:
                    if key in assistant_cache:
                        del assistant_cache[key]
                
                if expired_keys:
                    logger.log_with_context(
                        "Cleaned up expired assistant cache entries",
                        count=len(expired_keys)
                    )
                
        except Exception as e:
            logger.error_with_context(
                "Error during cleanup task",
                exc_info=True,
                error=str(e)
            )

def main():
    """
    Main function to process messages from the queue with enhanced health monitoring
    """
    global last_cleanup_time, last_health_check, last_message_received, consecutive_connection_errors
    
    if not AZURE_SERVICE_BUS_CONNECTION_STRING:
        logger.error("Azure Service Bus connection string is not set!")
        return
    
    logger.log_with_context(
        "Starting message processing",
        workers=MAX_WORKERS,
        queue=AZURE_SERVICE_BUS_QUEUE_NAME,
        database_type=DATABASE_TYPE
    )
    
    # Initialize time tracking variables
    last_cleanup_time = time.time()
    last_health_check = time.time()
    last_message_received = time.time()
    
    # Create thread pool for parallel processing
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        try:
            # Log startup event to Cosmos DB
            log_container_health_issue("container_startup", f"Container started with {MAX_WORKERS} workers")
            
            # Perform initial health check
            if not check_container_health():
                logger.warning("Initial health check failed, proceeding anyway...")
            
            # Main processing loop
            logger.log_with_context("Starting message processing loop")
            
            # Monitor metrics for processor health
            message_count = 0
            error_count = 0
            start_time = time.time()
            
            while True:
                current_time = time.time()
                
                # Run cleanup task
                cleanup_task()
                
                # Perform periodic health check
                if current_time - last_health_check > HEALTH_CHECK_INTERVAL:
                    health_check_result = check_container_health()
                    if not health_check_result and consecutive_connection_errors > MAX_CONNECTION_ERRORS:
                        logger.error(f"Health check failed {consecutive_connection_errors} times, restarting container")
                        restart_processing()
                    last_health_check = current_time
                    
                # Check if we haven't received messages for too long
                if current_time - last_message_received > NO_MESSAGES_TIMEOUT:
                    logger.warning(f"No messages received for {NO_MESSAGES_TIMEOUT/60:.1f} minutes, checking Service Bus connection")
                    if not check_container_health():
                        logger.error("Health check after message timeout failed, restarting container")
                        restart_processing()
                
                # Monitor thread health
                monitor_thread_health(executor)
                
                try:
                    # Use context manager for proper connection handling
                    servicebus_client = ServiceBusClient.from_connection_string(
                        AZURE_SERVICE_BUS_CONNECTION_STRING
                    )
                    with servicebus_client:
                        with servicebus_client.get_queue_receiver(
                            queue_name=AZURE_SERVICE_BUS_QUEUE_NAME,
                            max_wait_time=MAX_WAIT_TIME,
                            receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
                            prefetch_count=MAX_MESSAGE_COUNT
                        ) as receiver:
                            # New shared queue for message completion actions
                            message_actions = []
                            
                            # Receive batch of messages
                            messages = receiver.receive_messages(
                                max_message_count=MAX_MESSAGE_COUNT,
                                max_wait_time=MAX_WAIT_TIME
                            )
                            
                            if messages:
                                # Reset connection error counter on successful message receipt
                                consecutive_connection_errors = 0
                                last_message_received = time.time()
                                
                                batch_size = len(messages)
                                message_count += batch_size
                                logger.log_with_context(
                                    "Received messages batch",
                                    count=batch_size,
                                    total_processed=message_count
                                )
                                
                                # Submit each message to the thread pool and collect futures
                                futures = []
                                for message in messages:
                                    futures.append(executor.submit(process_message_in_thread, message, message_actions))
                                
                                # Wait for all futures to complete
                                concurrent.futures.wait(futures)
                                
                                # Process message actions in the main thread where the receiver is valid
                                with message_actions_lock:
                                    for message, action in message_actions:
                                        try:
                                            if action == 'complete':
                                                receiver.complete_message(message)
                                            elif action == 'abandon':
                                                receiver.abandon_message(message)
                                        except Exception as e:
                                            logger.error(f"Error performing message action {action}: {str(e)}")
                                    
                                    # Clear the action queue
                                    message_actions.clear()
                            else:
                                # No messages, sleep briefly before polling again
                                time.sleep(1)
                    
                    # Log metrics periodically (every hour)
                    current_time = time.time()
                    if current_time - start_time > 3600:  # 1 hour
                        uptime = current_time - start_time
                        logger.log_with_context(
                            "Processor metrics",
                            uptime_hours=round(uptime / 3600, 2),
                            messages_processed=message_count,
                            errors=error_count,
                            messages_per_hour=round(message_count / (uptime / 3600), 2),
                            active_requests=len(active_requests),
                            cached_assistants=len(assistant_cache),
                            connection_errors=consecutive_connection_errors
                        )
                        
                        # Also log health metrics to Cosmos DB
                        log_container_health_issue("metrics", json.dumps({
                            "uptime_hours": round(uptime / 3600, 2),
                            "messages_processed": message_count,
                            "errors": error_count,
                            "active_requests": len(active_requests),
                            "cached_assistants": len(assistant_cache),
                            "connection_errors": consecutive_connection_errors
                        }))
                        
                        # Reset counters but keep start_time for uptime calculation
                        message_count = 0
                        error_count = 0
                
                except (ServiceBusConnectionError, ServiceBusError) as sbe:
                    error_count += 1
                    consecutive_connection_errors += 1
                    logger.error_with_context(
                        "Service Bus connection error",
                        exc_info=True,
                        error=str(sbe),
                        consecutive_errors=consecutive_connection_errors
                    )
                    
                    # Log to Cosmos DB
                    log_container_health_issue("servicebus_error", str(sbe))
                    
                    # If we've had too many connection errors, restart
                    if consecutive_connection_errors > MAX_CONNECTION_ERRORS:
                        logger.error(f"Too many consecutive Service Bus errors ({consecutive_connection_errors}), restarting")
                        restart_processing()
                    
                    # Sleep longer for connection errors to allow recovery
                    time.sleep(CONNECTION_ERROR_SLEEP)
                    
                except Exception as batch_error:
                    error_count += 1
                    logger.error_with_context(
                        "Error receiving messages batch",
                        exc_info=True,
                        error=str(batch_error)
                    )
                    
                    # Log to Cosmos DB
                    log_container_health_issue("batch_error", str(batch_error))
                    
                    # If we've had too many errors, restart
                    if error_count > 20:
                        logger.error("Too many errors processing message batches, restarting")
                        restart_processing()
                    
                    # Brief pause to avoid tight loop in case of persistent errors
                    time.sleep(5)
                
        except KeyboardInterrupt:
            logger.log_with_context("Stopping message processing due to keyboard interrupt")
        except Exception as e:
            logger.error_with_context(
                "Error in message processing loop",
                exc_info=True,
                error=str(e)
            )
            # Log to Cosmos DB and attempt to restart
            log_container_health_issue("fatal_error", str(e))
            restart_processing()
        finally:
            # Flush any remaining conversations
            if len(pending_conversations) > 0:
                maybe_flush_conversation_batch(force=True)
                
            logger.log_with_context("Closing connections")
            
            # Log shutdown event to Cosmos DB
            log_container_health_issue("container_shutdown", "Container shutting down")

if __name__ == "__main__":
    logger.log_with_context("NL2SQL Queue Processor starting up")
    main()