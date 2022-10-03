import requests
import time

def handler(request):

    # Land Function
    try:
        start_time = time.time()
        url='***'
        resp = requests.post(url)
        duration=time.time() - start_time
        text=time.strftime("%H:%M:%S", time.gmtime(duration))
        text1=resp.text+' . Duration = '+text
    except:
        text1='Error in Load Function'  
    print(text1)
    
    # Stage Function
    try:
        start_time = time.time()
        url='***'
        resp = requests.post(url)
        duration=time.time() - start_time
        text=time.strftime("%H:%M:%S", time.gmtime(duration))
        text2=resp.text+' . Duration = '+text
    except:
        text2='Error in Stage Function'
    print(text2)
    
    # Prod DWH Function
    try:
        start_time = time.time()
        url='***'
        resp = requests.post(url)
        duration=time.time() - start_time
        text=time.strftime("%H:%M:%S", time.gmtime(duration))
        text3=resp.text+' . Duration = '+text
    except:
        text3='Error in Prod DWH Function'
    print(text3)    
    
    return text1,text2,text3

handler(1)