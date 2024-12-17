import asyncio
import aiohttp
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from asyncio import Semaphore
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
token_id = 'mz71n43h67yp3fni'
host = 'economyenergy.bitrix24.com.br'
user = 1

# Increased semaphore for more concurrent requests
semaphore = Semaphore(50)  # Increased concurrency
timeout = aiohttp.ClientTimeout(total=10)  # Reduced timeout to 10 seconds

class EmailRequest(BaseModel):
    email: str

async def fetch_with_retry(session, url, params, retries=3):
    """
    Fetch data with retries and exponential backoff in case of failure
    """
    for attempt in range(retries):
        try:
            async with session.post(url, json=params, timeout=timeout) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < retries - 1:
                delay = 2 ** attempt + random.uniform(0, 1)  # Exponential backoff
                await asyncio.sleep(delay)
            else:
                logger.error(f"Error fetching data after {retries} retries: {e}")
                raise e

async def fetch_all_items_optimized(session, url, params, item_type=''):
    """
    Optimized function to fetch ALL items from Bitrix24 API using ID-based pagination
    """
    all_items = []
    last_id = 0
    batch_size = 50  # Bitrix24 recommended batch size
    total_retrieved = 0

    # Modify params to use ID-based pagination
    base_params = params.copy()
    base_params['order'] = {'ID': 'ASC'}
    base_params['start'] = -1  # Disable count request for performance

    while True:
        # Update filter to get items after the last retrieved ID
        current_params = base_params.copy()
        current_params['filter'] = current_params.get('filter', {})
        current_params['filter']['>ID'] = last_id

        async with semaphore:
            try:
                data = await fetch_with_retry(session, url, current_params)
                
                if 'result' not in data or not data['result']:
                    logger.info(f"No more {item_type} items to retrieve.")
                    break

                # Add batch of items
                batch_items = data['result']
                all_items.extend(batch_items)
                total_retrieved += len(batch_items)

                # Log progress
                logger.info(f"Retrieved {total_retrieved} {item_type} items so far...")

                # Update last_id with the ID of the last item
                if batch_items:
                    last_id = max(item['ID'] for item in batch_items)

                # Stop condition: fewer items than batch size
                if len(batch_items) < batch_size:
                    logger.info(f"Finished retrieving {item_type} items. Total: {total_retrieved}")
                    break

            except Exception as e:
                logger.error(f"Error fetching {item_type} items: {e}")
                break

    return all_items

async def bulk_get_contact_phones(session, contact_ids):
    """
    Fetch contact phones in bulk to reduce API calls
    """
    if not contact_ids:
        return {}
    
    url = f'https://{host}/rest/{user}/{token_id}/crm.contact.list'
    
    params = {
        'filter': {'ID': contact_ids},
        'select': ['ID', 'PHONE'],
        'start': -1  # Optimize pagination
    }
    
    try:
        response_data = await fetch_all_items_optimized(session, url, params, item_type='contacts')
        
        # Create a mapping of contact_id to phone
        contact_phones = {
            str(contact['ID']): 
            (contact['PHONE'][0]['VALUE'] if contact.get('PHONE') and len(contact['PHONE']) > 0 else None)
            for contact in response_data
        }
        
        return contact_phones
    
    except Exception as e:
        logger.error(f"Error fetching contact phones in bulk: {e}")
        return {}

@app.post("/get_data")
async def get_data_endpoint(request: EmailRequest):
    async with aiohttp.ClientSession(timeout=timeout) as session:
        email_filter = request.email
        
        try:
            # Prepare URLs and parameters
            leads_url = f'https://{host}/rest/{user}/{token_id}/crm.lead.list/'
            deals_url = f'https://{host}/rest/{user}/{token_id}/crm.deal.list/'
            deals_usina_url = f'https://{host}/rest/{user}/{token_id}/crm.deal.list/'
            
            leads_params = {
                'filter': {'UF_CRM_1717008267006': email_filter},
                'select': ['ID', 'OPPORTUNITY', 'STATUS_ID', 'UF_CRM_1717008267006', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'PHONE', 'UF_CRM_1717008267006', 'UF_CRM_1716238809742', 'UF_CRM_1721931621996']
            }
            
            deals_params = {
                'filter': {'UF_CRM_6657792586A0F': email_filter},
                'select': ['PHONE', 'CATEGORY_ID', 'OPPORTUNITY', 'STAGE_ID', 'UF_CRM_6657792586A0F', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'UF_CRM_1716235306165', 'UF_CRM_1709207938786', 'UF_CRM_664BBA75E0765', 'UF_CRM_1709060681601', 'UF_CRM_1716236663328', 'UF_CRM_1716235986482']
            }
            
            deals_usina_params = {
                'filter': {'UF_CRM_1726088862520': email_filter},
                'select': ['TITLE', 'DATE_CREATE', 'CONTACT_ID', 'OPPORTUNITY', 'STAGE_ID', 'CATEGORY_ID', 'UF_CRM_1726088862520', 'UF_CRM_1726089384094', 'UF_CRM_1726089371430', 'UF_CRM_1726089284839']
            }
            
            # Fetch leads and deals concurrently
            leads_task = asyncio.create_task(fetch_all_items_optimized(session, leads_url, leads_params, item_type='leads'))
            deals_task = asyncio.create_task(fetch_all_items_optimized(session, deals_url, deals_params, item_type='deals'))
            deals_usina_task = asyncio.create_task(fetch_all_items_optimized(session, deals_usina_url, deals_usina_params, item_type='deals_usina'))
            
            leads, deals, deals_usina = await asyncio.gather(leads_task, deals_task, deals_usina_task)
            
            # Collect unique contact IDs from all sources
            contact_ids = list(set(
                [str(lead.get('CONTACT_ID', '')) for lead in leads if lead.get('CONTACT_ID')] +
                [str(deal.get('CONTACT_ID', '')) for deal in deals if deal.get('CONTACT_ID')] +
                [str(item.get('CONTACT_ID', '')) for item in deals_usina if item.get('CONTACT_ID')]
            ))
            
            # Fetch contact phones in bulk
            contact_phones = await bulk_get_contact_phones(session, contact_ids)
            
            # Process leads and add phone numbers
            for lead in leads:
                contact_id = str(lead.get('CONTACT_ID', ''))
                lead['PHONE'] = contact_phones.get(contact_id)
                lead['DATA_TYPE'] = 'LEAD'
            
            # Process deals and add phone numbers
            for deal in deals:
                contact_id = str(deal.get('CONTACT_ID', ''))
                deal['PHONE'] = contact_phones.get(contact_id)
                deal['DATA_TYPE'] = 'DEAL'
            
            # Process novo items and add phone numbers
            for item in deals_usina:
                contact_id = str(item.get('CONTACT_ID', ''))
                item['PHONE'] = contact_phones.get(contact_id)
                item['DATA_TYPE'] = 'DEALS_USINA'
            
            # Combine leads, deals, and novo items
            all_data = leads + deals + deals_usina
            
            logger.info(f"Total data retrieved: {len(all_data)} items")
            logger.info(f"Leads: {len(leads)}, Deals: {len(deals)}, Deals Usina: {len(deals_usina)}")
            
            return {
                "data": all_data, 
                "total_count": len(all_data), 
                "leads_count": len(leads), 
                "deals_count": len(deals), 
                "deals_usina_count": len(deals_usina),
                "statusbody": 200
            }
        
        except Exception as e:
            logger.error(f"Error processing data for email {email_filter}: {e}")
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
