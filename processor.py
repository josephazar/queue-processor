import os
import json
import time
import asyncio
import traceback
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiveMode
from azure.servicebus.exceptions import ServiceBusConnectionError, ServiceBusError
import importlib.util
from pathlib import Path
from openai import AzureOpenAI
import openai

# Import configuration
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

# Import database functions
from database import (
    store_request, 
    update_request_status, 
    cleanup_old_requests, 
    get_request_status,
    store_conversation,
    get_assistant_last_activity,
    cleanup_old_conversations,
    bulk_store_conversations,
    log_container_health_issue,
    # New functions for assistant pool persistence
    get_pool_assistants,
    store_pool_assistant,
    remove_pool_assistant
)

# Import logging utils
from logging_utils import setup_logging, verify_logging_paths, init_logging

# Check if src/main.py exists and import initialize_assistant
main_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src", "main.py")
if os.path.exists(main_path):
    spec = importlib.util.spec_from_file_location("main", main_path)
    main_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(main_module)
    initialize_assistant = main_module.initialize_assistant
else:
    raise ImportError("Could not find src/main.py which contains initialize_assistant")

# Set up logging
logger = init_logging()
logger.info("NL2SQL Queue Processor starting up")

# Validate configuration
validate_config()

# Assistant Pool Configuration
ASSISTANT_POOL_SIZE = int(os.getenv("ASSISTANT_POOL_SIZE", "5"))
THREAD_LIFETIME_HOURS = int(os.getenv("THREAD_LIFETIME_HOURS", "24"))
THREAD_LIFETIME_SECONDS = THREAD_LIFETIME_HOURS * 3600

# Thread safety mechanisms
active_requests = {}
active_requests_lock = threading.RLock()

# Assistant pool and thread management
assistant_pool = []
assistant_pool_lock = threading.RLock()
assistant_assignments = {}  # Maps assistant_id -> {user_email, thread_id, last_used}
thread_cache = {}  # Maps user_email -> {assistant_id, thread_id, created_at}
thread_cache_lock = threading.RLock()

# Message batch tracking for bulk operations
pending_conversations = []
pending_conversations_lock = threading.RLock()

# Health monitoring variables
last_health_check = 0
last_message_received = 0
last_cleanup_time = 0
consecutive_connection_errors = 0

# Constants
BULK_INSERT_THRESHOLD = 10  # Number of conversations to batch before inserting
HEALTH_CHECK_INTERVAL = 2700  # 45 minutes
NO_MESSAGES_TIMEOUT = 1800  # 30 minutes (no messages received)
MAX_CONNECTION_ERRORS = 10  # Max consecutive connection errors before restart
CONNECTION_ERROR_SLEEP = 10  # Seconds to sleep after a connection error

def initialize_assistant_pool():
    """
    Initialize a pool of assistants, retrieving existing ones from Cosmos DB or creating new ones
    """
    global assistant_pool
    
    logger.info(f"Initializing assistant pool with target size of {ASSISTANT_POOL_SIZE} assistants")
    
    # First, check if we have existing assistants in Cosmos DB
    existing_assistants = get_pool_assistants()
    
    if existing_assistants:
        logger.info(f"Found {len(existing_assistants)} existing assistants in Cosmos DB")
        
        # Verify each assistant exists in OpenAI
        valid_assistants = []
        for assistant_id in existing_assistants:
            try:
                # Try to retrieve the assistant to verify it exists
                client = AzureOpenAI(
                    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
                    azure_endpoint=os.getenv("AZURE_OPENAI_API_ENDPOINT"),
                )
                client.beta.assistants.retrieve(assistant_id)
                
                # If successful, add to our valid assistants
                valid_assistants.append(assistant_id)
                logger.info(f"Verified existing assistant: {assistant_id}")
                
            except Exception as e:
                logger.warning(f"Assistant {assistant_id} from DB no longer exists in OpenAI: {e}")
                # Remove from Cosmos DB since it's no longer valid
                remove_pool_assistant(assistant_id)
        
        # Add valid assistants to our pool
        with assistant_pool_lock:
            assistant_pool = valid_assistants
            for assistant_id in assistant_pool:
                assistant_assignments[assistant_id] = {
                    "user_email": None,
                    "thread_id": None,
                    "last_used": None,
                    "in_use": False
                }
    
    # Check if we need to create additional assistants to reach the target size
    assistants_to_create = max(0, ASSISTANT_POOL_SIZE - len(assistant_pool))
    
    if assistants_to_create > 0:
        logger.info(f"Creating {assistants_to_create} new assistants to reach target pool size")
        
        created_count = 0
        for i in range(assistants_to_create):
            try:
                assistant = initialize_assistant(DATABASE_TYPE)
                assistant_id = assistant.assistant.assistant_id
                
                # Store in Cosmos DB for persistence
                store_pool_assistant(assistant_id)
                
                with assistant_pool_lock:
                    assistant_pool.append(assistant_id)
                    assistant_assignments[assistant_id] = {
                        "user_email": None,
                        "thread_id": None,
                        "last_used": None,
                        "in_use": False
                    }
                    
                created_count += 1
                logger.info(f"Created and stored new assistant {i+1}/{assistants_to_create} with ID: {assistant_id}")
                
            except Exception as e:
                logger.error(f"Failed to create assistant {i+1}/{assistants_to_create}: {str(e)}", exc_info=True)
    
    # Log final pool status
    with assistant_pool_lock:
        logger.info(f"Assistant pool initialized with {len(assistant_pool)}/{ASSISTANT_POOL_SIZE} assistants")
    
    return len(assistant_pool) > 0

def get_available_assistant(user_email):
    """
    Get an available assistant from the pool or the user's existing assignment
    
    Args:
        user_email: The user's email to find an existing assistant or assign a new one
        
    Returns:
        tuple: (assistant_id, thread_id, is_new_thread)
    """
    # First check if the user already has a thread in the cache
    with thread_cache_lock:
        if user_email in thread_cache:
            thread_info = thread_cache[user_email]
            assistant_id = thread_info["assistant_id"]
            thread_id = thread_info["thread_id"]
            created_at = thread_info["created_at"]
            
            # Check if thread has expired
            if time.time() - created_at > THREAD_LIFETIME_SECONDS:
                logger.info(f"Thread for user {user_email} has expired, will assign new thread")
                # Remove from thread cache
                del thread_cache[user_email]
                # But keep assistant_id for reassignment
            else:
                # Thread still valid
                with assistant_pool_lock:
                    # Update last_used time
                    if assistant_id in assistant_assignments:
                        assistant_assignments[assistant_id]["last_used"] = time.time()
                
                logger.info(f"Reusing existing thread for user {user_email} with assistant {assistant_id}")
                return assistant_id, thread_id, False
    
    # Need to assign an assistant and create a new thread
    with assistant_pool_lock:
        # First try to find an unassigned assistant
        for assistant_id in assistant_pool:
            if not assistant_assignments[assistant_id]["in_use"]:
                # Assign this assistant to the user
                assistant_assignments[assistant_id] = {
                    "user_email": user_email,
                    "thread_id": None,  # Will be set after thread creation
                    "last_used": time.time(),
                    "in_use": True
                }
                logger.info(f"Assigned available assistant {assistant_id} to user {user_email}")
                return assistant_id, None, True
        
        # If all assistants are in use, find the least recently used
        least_recent_time = float('inf')
        least_recent_assistant = None
        
        for assistant_id in assistant_pool:
            last_used = assistant_assignments[assistant_id]["last_used"]
            if last_used is not None and last_used < least_recent_time:
                least_recent_time = last_used
                least_recent_assistant = assistant_id
        
        if least_recent_assistant:
            # Re-assign this assistant to the new user
            assistant_assignments[least_recent_assistant] = {
                "user_email": user_email,
                "thread_id": None,  # Will be set after thread creation
                "last_used": time.time(),
                "in_use": True
            }
            logger.info(f"Reassigned least recently used assistant {least_recent_assistant} to user {user_email}")
            return least_recent_assistant, None, True
    
    # If we get here, something went wrong - no assistants available
    logger.error("No assistants available in pool. This should never happen!")
    
    # Emergency fallback: create a new assistant outside the pool
    try:
        logger.warning("Creating emergency assistant outside pool")
        assistant = initialize_assistant(DATABASE_TYPE)
        return assistant.assistant.assistant_id, None, True
    except Exception as e:
        logger.error(f"Failed to create emergency assistant: {str(e)}", exc_info=True)
        raise RuntimeError("Failed to get an assistant for the user")

def update_thread_assignment(assistant_id, thread_id, user_email):
    """
    Update the assistant and thread assignments after thread creation
    
    Args:
        assistant_id: The assistant ID
        thread_id: The newly created thread ID
        user_email: The user's email
    """
    # Update assistant assignment
    with assistant_pool_lock:
        if assistant_id in assistant_assignments:
            assistant_assignments[assistant_id]["thread_id"] = thread_id
            assistant_assignments[assistant_id]["last_used"] = time.time()
    
    # Update thread cache
    with thread_cache_lock:
        thread_cache[user_email] = {
            "assistant_id": assistant_id,
            "thread_id": thread_id,
            "created_at": time.time()
        }
    
    logger.info(f"Updated thread assignment: assistant={assistant_id}, thread={thread_id}, user={user_email}")

def release_assistant(assistant_id):
    """
    Mark an assistant as no longer in use
    
    Args:
        assistant_id: The assistant ID to release
    """
    with assistant_pool_lock:
        if assistant_id in assistant_assignments:
            user_email = assistant_assignments[assistant_id]["user_email"]
            assistant_assignments[assistant_id] = {
                "user_email": None,
                "thread_id": None,
                "last_used": time.time(),
                "in_use": False
            }
            logger.info(f"Released assistant {assistant_id} from user {user_email}")
            
            # Also remove from thread cache
            with thread_cache_lock:
                for email, info in list(thread_cache.items()):
                    if info["assistant_id"] == assistant_id:
                        del thread_cache[email]
                        logger.info(f"Removed thread cache entry for user {email}")
                        break

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
        
        # Check assistant pool health
        with assistant_pool_lock:
            if len(assistant_pool) < ASSISTANT_POOL_SIZE * 0.5:
                logger.warning(f"Assistant pool size is critically low: {len(assistant_pool)}/{ASSISTANT_POOL_SIZE}")
                log_container_health_issue("assistant_pool_depleted", 
                                          f"Only {len(assistant_pool)}/{ASSISTANT_POOL_SIZE} assistants in pool")
                # Try to replenish the pool
                replenish_assistant_pool()
        
        # All checks passed
        consecutive_connection_errors = 0
        logger.info("All container health checks passed")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error during health check: {str(e)}", exc_info=True)
        log_container_health_issue("health_check_error", str(e))
        consecutive_connection_errors += 1
        return False

def replenish_assistant_pool():
    """
    Check and replenish the assistant pool if needed
    """
    with assistant_pool_lock:
        current_pool_size = len(assistant_pool)
        assistants_to_add = max(0, ASSISTANT_POOL_SIZE - current_pool_size)
        
        if assistants_to_add > 0:
            logger.info(f"Replenishing assistant pool, adding {assistants_to_add} assistants")
            
            added_count = 0
            for i in range(assistants_to_add):
                try:
                    assistant = initialize_assistant(DATABASE_TYPE)
                    assistant_id = assistant.assistant.assistant_id
                    
                    # Store in Cosmos DB for persistence
                    store_pool_assistant(assistant_id)
                    
                    assistant_pool.append(assistant_id)
                    assistant_assignments[assistant_id] = {
                        "user_email": None,
                        "thread_id": None,
                        "last_used": None,
                        "in_use": False
                    }
                    
                    added_count += 1
                    logger.info(f"Added new assistant to pool: {assistant_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to add assistant to pool: {str(e)}", exc_info=True)
            
            logger.info(f"Added {added_count}/{assistants_to_add} assistants to pool. New size: {current_pool_size + added_count}")

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
    Delete an assistant by ID, handling the case where the assistant doesn't exist
    """
    try:
        if not assistant_id:
            return {"status": "error", "message": "No assistant ID provided"}
        
        # Check if this assistant is in our pool - if so, don't delete it
        with assistant_pool_lock:
            if assistant_id in assistant_pool:
                logger.info(f"Not deleting assistant {assistant_id} because it's in the pool - just releasing it")
                release_assistant(assistant_id)
                return {
                    "status": "success", 
                    "message": f"Assistant {assistant_id} released (not deleted because it's in the pool)",
                    "assistant_id": None,
                    "thread_id": None
                }
        
        # The assistant is not in our pool, so it's safe to delete
        # Construct the API URL from environment variables
        endpoint = os.getenv("AZURE_OPENAI_API_ENDPOINT")
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        url = f"{endpoint}/openai/assistants/{assistant_id}?api-version={api_version}"

        # Import requests here to avoid global import
        import requests
        
        # Make the DELETE request
        response = requests.delete(url, headers={"api-key": os.getenv("AZURE_OPENAI_API_KEY")})

        # Handle 404 - assistant already doesn't exist
        if response.status_code == 404:
            logger.info(f"Assistant already deleted or doesn't exist: {assistant_id}")
            
            return {
                "status": "success", 
                "message": f"Assistant {assistant_id} already deleted or doesn't exist",
                "assistant_id": None,
                "thread_id": None
            }
        
        # Handle success cases
        elif response.status_code in [200, 201, 202, 204]:
            logger.info(f"Assistant deleted successfully: {assistant_id}")
            
            return {
                "status": "success", 
                "message": f"Assistant {assistant_id} deleted successfully",
                "assistant_id": None,
                "thread_id": None
            }
        else:
            error_msg = f"Error deleting assistant: {response.text}"
            logger.error(f"{error_msg}, assistant_id={assistant_id}")
            return {
                "status": "error",
                "message": error_msg,
                "assistant_id": assistant_id,
                "thread_id": None
            }
    except Exception as e:
        logger.error(f"Error deleting assistant: {str(e)}", exc_info=True)
        return {
            "status": "error", 
            "message": f"Error deleting assistant: {str(e)}",
            "assistant_id": assistant_id,
            "thread_id": None
        }

def maybe_flush_conversation_batch(force=False):
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
            logger.info(f"Bulk inserted {count} conversations")
        except Exception as e:
            logger.error(f"Failed to bulk insert conversations: {str(e)}", exc_info=True)
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
                    logger.error(f"Failed to store individual conversation: {str(inner_e)}")

async def process_question(request_id, question, assistant_id=None, thread_id=None, user_email=None, request_type=None, report_name=None):
    """
    Process a user question using the NL2SQL assistant.
    This is the core function that communicates with the AI model.
    """
    start_time = time.time()
    new_thread_created = False  # Track if we created a new thread
    
    try:
        logger.info(f"Processing question for request_id={request_id}, user_email={user_email}")
        
        # Check if this is a termination request
        if any(word in question.lower().split() for word in ["bye", "exit", "end"]):
            logger.info(f"Received termination request from user_email={user_email}")
            
            # Don't delete assistants in pool, just release the assignment
            if assistant_id:
                with assistant_pool_lock:
                    if assistant_id in assistant_pool:
                        release_assistant(assistant_id)
                        
                        # Also clear thread cache for this user
                        with thread_cache_lock:
                            if user_email in thread_cache:
                                del thread_cache[user_email]
                        
                        return {
                            "status": "success", 
                            "message": "Goodbye! Your conversation has ended.", 
                            "assistant_id": None, 
                            "thread_id": None
                        }
                    else:
                        # Not in our pool, safe to delete
                        return await delete_assistant(assistant_id)
            else:
                return {
                    "status": "success", 
                    "message": "Goodbye!", 
                    "assistant_id": None, 
                    "thread_id": None
                }
        
        # Get an assistant from the pool or reuse existing assignment
        if not assistant_id and not thread_id and user_email:
            assistant_id, thread_id, is_new_thread = get_available_assistant(user_email)
            new_thread_created = is_new_thread
        
        # Initialize assistant - will be from pool
        try:
            logger.info(f"Using assistant_id={assistant_id}")
            sql_assistant = initialize_assistant(DATABASE_TYPE, assistant_id=assistant_id)
        except openai.NotFoundError:
            # The assistant ID doesn't exist anymore (shouldn't happen with pool)
            logger.error(f"Assistant not found: {assistant_id}. This shouldn't happen with the pool approach.")
            
            # Remove from pool and DB since it no longer exists
            with assistant_pool_lock:
                if assistant_id in assistant_pool:
                    assistant_pool.remove(assistant_id)
                    del assistant_assignments[assistant_id]
            
            # Remove from Cosmos DB
            remove_pool_assistant(assistant_id)
            
            # Try to get a different assistant from the pool
            assistant_id, thread_id, is_new_thread = get_available_assistant(user_email)
            new_thread_created = is_new_thread
            
            # Initialize with the new assistant
            sql_assistant = initialize_assistant(DATABASE_TYPE, assistant_id=assistant_id)
            logger.info(f"Created replacement assistant: {assistant_id}")

        # Create a thread if needed
        if not thread_id:
            thread = sql_assistant.assistant.create_thread()
            thread_id = thread.id
            new_thread_created = True
            logger.info(f"Created new thread: {thread_id} for assistant: {assistant_id}")
            
            # Update assignments
            if user_email:
                update_thread_assignment(assistant_id, thread_id, user_email)
        
        # Process the question
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
                logger.warning(f"Request processing took longer than timeout: {processing_duration}s > {timeout}s")
            
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
            
            # Update the last_used time for this assistant
            with assistant_pool_lock:
                if assistant_id in assistant_assignments:
                    assistant_assignments[assistant_id]["last_used"] = time.time()
            
            # Calculate total processing time
            total_duration = time.time() - start_time
            logger.info(f"Completed processing question in {total_duration:.2f}s")
            
            # Return the response with assistant and thread IDs
            return {
                "status": "success", 
                "response": answer,
                "assistant_id": assistant_id,
                "thread_id": thread_id
            }
        except Exception as e:
            logger.error(f"Error calling create_response: {str(e)}", exc_info=True)
            
            # If we created a new thread but there was an error, we should clean up
            if new_thread_created:
                # Don't clean up the assistant (it's in the pool) but make it available again
                with assistant_pool_lock:
                    if assistant_id in assistant_assignments:
                        assistant_assignments[assistant_id]["in_use"] = False
                        assistant_assignments[assistant_id]["thread_id"] = None
                
                # Remove from thread cache if present
                with thread_cache_lock:
                    if user_email in thread_cache and thread_cache[user_email]["thread_id"] == thread_id:
                        del thread_cache[user_email]
            
            return {
                "status": "error",
                "message": f"Error processing question: {str(e)}",
                "assistant_id": assistant_id,  # Keep the assistant_id since it's from the pool
                "thread_id": None  # Clear the thread_id since there was an error
            }
    
    except Exception as e:
        logger.error(f"Error processing question: {str(e)}", exc_info=True)
        
        return {
            "status": "error",
            "message": f"Error processing question: {str(e)}",
            "assistant_id": assistant_id,  # Keep the assistant_id if it exists
            "thread_id": None
        }

def process_message(message, action_queue):
    """
    Process a single message from the queue with improved thread safety
    Returns action to take (complete or abandon)
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
        report_name = body.get("report_name")
        
        if not request_id or not question:
            logger.error(f"Message missing required fields: {body.keys()}")
            return "complete"  # Skip invalid messages
        
        # Thread safety: Check if this request is already being processed
        with active_requests_lock:
            if request_id in active_requests:
                logger.info(f"Request {request_id} already being processed, skipping")
                return "complete"
            
            # Mark this request as being processed
            active_requests[request_id] = time.time()
            processing_started = True
        
        # Check the current status of the request
        current_status = get_request_status(request_id)
        if current_status and current_status.get("status") in ["completed", "error"]:
            logger.info(f"Request {request_id} already processed with status {current_status.get('status')}, skipping")
            
            # Remove from active requests
            with active_requests_lock:
                if request_id in active_requests:
                    del active_requests[request_id]
            return "complete"
        
        logger.info(f"Processing request {request_id} for user {user_email}")
        
        # Update the request status to processing
        update_request_status(request_id, "processing")
        
        # Process the question
        result = run_async_in_thread(process_question,
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
        
        # Calculate total processing time
        total_duration = time.time() - start_time
        logger.info(f"Completed processing request {request_id} in {total_duration:.2f}s with status {status}")
        
        return "complete"
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}", exc_info=True)
        
        # Update status if we have a request_id
        if request_id:
            error_result = {
                "status": "error",
                "error": str(e),
                "stacktrace": traceback.format_exc()
            }
            update_request_status(request_id, "error", error_result)
        
        return "abandon"  # Return to queue for retry
    
    finally:
        # Always clean up the active requests tracker
        if processing_started and request_id:
            with active_requests_lock:
                if request_id in active_requests:
                    del active_requests[request_id]
                    
        # Force flush any pending conversations regardless of count
        maybe_flush_conversation_batch(force=True)

def run_async_in_thread(async_func, *args, **kwargs):
    """
    Run an async function in a separate thread using a new event loop
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(async_func(*args, **kwargs))
    finally:
        loop.close()

def process_message_in_thread(message, _):
    """
    Wrapper to process message in a thread
    Returns the action to take (complete or abandon)
    """
    return run_async_in_thread(process_message, message, None)

def cleanup_task():
    """
    Periodically clean up old requests and expired threads
    """
    global last_cleanup_time
    
    current_time = time.time()
    time_since_last_cleanup = current_time - last_cleanup_time
    
    # Ensure all pending conversations are flushed first
    if len(pending_conversations) > 0:
        maybe_flush_conversation_batch(force=True)
    
    # Convert hours to seconds for comparison
    cleanup_interval_seconds = CLEANUP_INTERVAL_HOURS * 3600
    
    if time_since_last_cleanup >= cleanup_interval_seconds:
        logger.info(f"Running cleanup task")
        try:
            # Clean up old requests
            deleted_items = cleanup_old_requests(CLEANUP_DAYS)
            logger.info(f"Cleaned up {len(deleted_items)} old requests")
            
            # Clean up old conversations (keep for 30 days by default)
            deleted_conversations = cleanup_old_conversations(30)
            logger.info(f"Cleaned up {deleted_conversations} old conversations")
            
            # Clean up old log files (keep for 15 days)
            from logging_utils import cleanup_old_logs
            deleted_logs = cleanup_old_logs(LOGS_DIR, max_days=15)
            logger.info(f"Cleaned up {deleted_logs} old log files")
            
            # Check log directory size
            from logging_utils import check_log_directory_size
            check_log_directory_size(LOGS_DIR, max_size_gb=2)
            
            # Update last cleanup time
            last_cleanup_time = current_time
            
            # Clean up expired threads from thread cache
            with thread_cache_lock:
                expired_count = 0
                for user_email, info in list(thread_cache.items()):
                    thread_age = current_time - info["created_at"]
                    if thread_age > THREAD_LIFETIME_SECONDS:
                        del thread_cache[user_email]
                        expired_count += 1
                
                if expired_count > 0:
                    logger.info(f"Cleaned up {expired_count} expired threads from cache")
            
            # Ensure assistant pool is at full capacity
            replenish_assistant_pool()
            
            # Verify each assistant in the pool still exists
            with assistant_pool_lock:
                for assistant_id in list(assistant_pool):
                    try:
                        # Try to retrieve the assistant to verify it exists
                        client = AzureOpenAI(
                            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
                            azure_endpoint=os.getenv("AZURE_OPENAI_API_ENDPOINT"),
                        )
                        client.beta.assistants.retrieve(assistant_id)
                    except Exception as e:
                        logger.warning(f"Assistant {assistant_id} no longer exists: {e}")
                        # Remove from pool
                        assistant_pool.remove(assistant_id)
                        if assistant_id in assistant_assignments:
                            del assistant_assignments[assistant_id]
                        # Remove from Cosmos DB
                        remove_pool_assistant(assistant_id)
            
        except Exception as e:
            logger.error(f"Error during cleanup task: {str(e)}", exc_info=True)

def main():
    """
    Main function to process messages from the queue with enhanced health monitoring
    """
    global last_cleanup_time, last_health_check, last_message_received, consecutive_connection_errors
    
    if not AZURE_SERVICE_BUS_CONNECTION_STRING:
        logger.error("Azure Service Bus connection string is not set!")
        return
    
    # Verify logging paths are set correctly
    result = verify_logging_paths()
    if not result:
        logger.error("Failed to verify logging paths! Logs may not be written correctly.")
    
    logger.info(f"Starting message processing with {MAX_WORKERS} workers")
    
    # Initialize the assistant pool
    pool_initialized = initialize_assistant_pool()
    if not pool_initialized:
        logger.error("Failed to initialize assistant pool, exiting")
        return
    
    # Initialize time tracking variables
    last_cleanup_time = time.time()
    last_health_check = time.time()
    last_message_received = time.time()
    
    # Create thread pool for parallel processing
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        try:
            # Log startup event to Cosmos DB
            log_container_health_issue("container_startup", f"Container started with {MAX_WORKERS} workers, {len(assistant_pool)}/{ASSISTANT_POOL_SIZE} assistants in pool")
            
            # Perform initial health check
            if not check_container_health():
                logger.warning("Initial health check failed, proceeding anyway...")
            
            # Main processing loop
            logger.info("Starting message processing loop")
            
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
                                logger.info(f"Received batch of {batch_size} messages")
                                
                                # Submit each message to the thread pool and collect futures
                                futures = []
                                message_actions = {}  # Map of messages to actions
                                
                                for message in messages:
                                    future = executor.submit(process_message, message, None)
                                    futures.append((future, message))
                                
                                # Wait for all futures to complete
                                completed, _ = wait([f[0] for f in futures])
                                
                                # Process message actions in the main thread where the receiver is valid
                                for future, message in futures:
                                    if future.done():
                                        try:
                                            action = future.result()
                                            if action == "complete":
                                                receiver.complete_message(message)
                                            elif action == "abandon":
                                                receiver.abandon_message(message)
                                        except Exception as e:
                                            logger.error(f"Error performing message action: {str(e)}")
                                            # Default to abandoning the message if we can't process the action
                                            try:
                                                receiver.abandon_message(message)
                                            except Exception:
                                                pass
                            else:
                                # No messages, sleep briefly before polling again
                                time.sleep(1)
                    
                    # Log metrics periodically (every hour)
                    current_time = time.time()
                    if current_time - start_time > 3600:  # 1 hour
                        uptime = current_time - start_time
                        
                        # Count active assistants
                        with assistant_pool_lock:
                            active_assistants = sum(1 for a in assistant_assignments.values() if a["in_use"])
                        
                        # Count active threads
                        with thread_cache_lock:
                            active_threads = len(thread_cache)
                        
                        logger.info(
                            f"Processor metrics - Uptime: {uptime/3600:.2f}h, "
                            f"Messages: {message_count}, Errors: {error_count}, "
                            f"Active requests: {len(active_requests)}, "
                            f"Pool: {len(assistant_pool)}/{ASSISTANT_POOL_SIZE}, "
                            f"Active assistants: {active_assistants}, "
                            f"Active threads: {active_threads}"
                        )
                        
                        # Also log health metrics to Cosmos DB
                        log_container_health_issue("metrics", json.dumps({
                            "uptime_hours": round(uptime / 3600, 2),
                            "messages_processed": message_count,
                            "errors": error_count,
                            "active_requests": len(active_requests),
                            "assistant_pool_size": len(assistant_pool),
                            "assistant_pool_capacity": ASSISTANT_POOL_SIZE,
                            "active_assistants": active_assistants,
                            "active_threads": active_threads,
                            "connection_errors": consecutive_connection_errors
                        }))
                        
                        # Reset counters but keep start_time for uptime calculation
                        message_count = 0
                        error_count = 0
                
                except (ServiceBusConnectionError, ServiceBusError) as sbe:
                    error_count += 1
                    consecutive_connection_errors += 1
                    logger.error(f"Service Bus connection error: {str(sbe)}")
                    
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
                    logger.error(f"Error receiving messages batch: {str(batch_error)}")
                    
                    # Log to Cosmos DB
                    log_container_health_issue("batch_error", str(batch_error))
                    
                    # If we've had too many errors, restart
                    if error_count > 20:
                        logger.error("Too many errors processing message batches, restarting")
                        restart_processing()
                    
                    # Brief pause to avoid tight loop in case of persistent errors
                    time.sleep(5)
                
        except KeyboardInterrupt:
            logger.info("Stopping message processing due to keyboard interrupt")
        except Exception as e:
            logger.error(f"Error in message processing loop: {str(e)}", exc_info=True)
            # Log to Cosmos DB and attempt to restart
            log_container_health_issue("fatal_error", str(e))
            restart_processing()
        finally:
            # Flush any remaining conversations
            if len(pending_conversations) > 0:
                maybe_flush_conversation_batch(force=True)
                
            logger.info("Closing connections")
            
            # Log shutdown event to Cosmos DB
            log_container_health_issue("container_shutdown", "Container shutting down")

if __name__ == "__main__":
    logger.info("NL2SQL Queue Processor starting up")
    main()