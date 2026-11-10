import asyncio, ipaddress, os, re, socket, tempfile
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
import aiohttp
from aiohttp.abc import AbstractResolver
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

app=FastAPI(title='MonDrop API',version='4.0')
origins=[x.strip() for x in os.getenv('ALLOWED_ORIGINS','').split(',') if x.strip()]
app.add_middleware(CORSMiddleware,allow_origins=origins,allow_methods=['GET','POST'],allow_headers=['Content-Type'],expose_headers=['Content-Disposition','Content-Length'])
MAX_BYTES=100*1024*1024
MAX_CONCURRENT=2
slots=asyncio.Semaphore(MAX_CONCURRENT)
class Request(BaseModel):
    url:str
    format:str='mp4'

class SafeResolver(AbstractResolver):
    async def resolve(self,host,port=0,family=socket.AF_INET):
        loop=asyncio.get_running_loop()
        try: results=await loop.getaddrinfo(host,port,type=socket.SOCK_STREAM)
        except socket.gaierror: raise ValueError('Alan adı bulunamadı')
        addresses={r[4][0] for r in results}
        if not addresses or any(not ipaddress.ip_address(x).is_global for x in addresses): raise ValueError('Güvenli olmayan ağ adresi')
        return [{'hostname':host,'host':ip,'port':port,'family':socket.AF_INET6 if ':' in ip else socket.AF_INET,'proto':0,'flags':0} for ip in addresses]
    async def close(self): pass

def check_url(url):
    p=urlsplit(url)
    if p.scheme!='https' or not p.hostname or p.username or p.password or p.port not in (None,443):
        raise HTTPException(400,'Yalnızca standart HTTPS bağlantıları kabul edilir.')
    if len(url)>2048: raise HTTPException(400,'Bağlantı çok uzun.')
    if p.hostname.lower().rstrip('.') in ('localhost',) or p.hostname.endswith(('.local','.internal')):
        raise HTTPException(400,'Yerel adresler kabul edilmez.')
    return p

@app.get('/')
def root(): return {'service':'MonDrop API','status':'online','version':'4.0'}
@app.get('/health')
def health(): return {'ok':True}

def youtube_video_id(url):
    """Accept only known YouTube hosts and canonical video-ID URL forms."""
    p=urlsplit(url)
    host=(p.hostname or '').lower().rstrip('.')
    parts=[x for x in p.path.split('/') if x]
    video_id=None
    if host in ('youtu.be','www.youtu.be') and len(parts)==1:
        video_id=parts[0]
    elif host in ('youtube.com','www.youtube.com','m.youtube.com','music.youtube.com'):
        if parts==['watch']:
            video_id=parse_qs(p.query).get('v',[None])[0]
        elif len(parts)==2 and parts[0] in ('shorts','live','embed'):
            video_id=parts[1]
    if video_id and re.fullmatch(r'[A-Za-z0-9_-]{11}',video_id):
        return video_id
    return None

async def download_youtube(req):
    video_id=youtube_video_id(req.url)
    if not video_id:
        raise HTTPException(422,'Geçerli bir YouTube video bağlantısı girin.')
    temp=tempfile.TemporaryDirectory(prefix='mondrop-yt-')
    base=Path(temp.name)
    target=base/('MonDrop.'+req.format)
    # No cookies, login, proxies, or access-control circumvention.
    canonical='https://www.youtube.com/watch?v='+video_id
    args=['yt-dlp','--no-playlist','--no-warnings','--no-progress',
          '--no-cache-dir','--socket-timeout','15','--retries','1',
          '--max-filesize','90M','--match-filter','duration <= 1200',
          '--output',str(base/'MonDrop.%(ext)s')]
    if req.format=='mp3':
        args+=['--format','bestaudio/best','--extract-audio','--audio-format','mp3','--audio-quality','192K']
    else:
        args+=['--format','bestvideo[height<=720]+bestaudio/best[height<=720]/best',
               '--merge-output-format','mp4','--recode-video','mp4']
    args.append(canonical)
    try:
        async with slots:
            proc=await asyncio.create_subprocess_exec(*args,stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.PIPE)
            try:
                _,stderr=await asyncio.wait_for(proc.communicate(),timeout=175)
            except asyncio.TimeoutError:
                proc.kill();await proc.communicate()
                raise HTTPException(504,'İşlem zaman aşımına uğradı. Daha kısa bir video deneyin.')
            if proc.returncode!=0 or not target.is_file():
                raise HTTPException(422,'YouTube bu videonun indirilmesine izin vermedi veya sunucu videoyu işleyemedi.')
            if target.stat().st_size>MAX_BYTES:
                raise HTTPException(413,'Çıktı 100 MB sınırını aşıyor.')
        return FileResponse(target,media_type='audio/mpeg' if req.format=='mp3' else 'video/mp4',
                            filename='MonDrop.'+req.format,background=BackgroundTask(temp.cleanup))
    except Exception:
        temp.cleanup()
        raise

@app.post('/download')
async def download(req:Request):
    p=check_url(req.url)
    if req.format not in ('mp3','mp4'): raise HTTPException(400,'Geçersiz format.')
    if youtube_video_id(req.url):
        return await download_youtube(req)
    # Other platform pages are not supported.
    host=p.hostname.lower()
    if any(host==d or host.endswith('.'+d) for d in ('youtube.com','youtu.be','instagram.com','tiktok.com','facebook.com','vimeo.com','twitter.com','x.com')):
        raise HTTPException(422,'Platform sayfaları desteklenmiyor; doğrudan ve izinli medya dosyası bağlantısı kullanın.')
    temp=tempfile.TemporaryDirectory(prefix='mondrop-')
    base=Path(temp.name)
    src=base/'input'; dest=base/('mondrop.'+req.format)
    try:
        async with slots:
            connector=aiohttp.TCPConnector(resolver=SafeResolver(),use_dns_cache=False,limit=2)
            timeout=aiohttp.ClientTimeout(total=150,connect=15,sock_read=30)
            async with aiohttp.ClientSession(connector=connector,timeout=timeout,trust_env=False) as session:
                async with session.get(req.url,allow_redirects=False,headers={'Accept':'video/*,audio/*'}) as response:
                    if response.status in (301,302,303,307,308): raise HTTPException(422,'Yönlendirmeli bağlantılar güvenlik nedeniyle desteklenmiyor.')
                    if response.status!=200: raise HTTPException(422,f'Kaynak sunucu HTTP {response.status} yanıtı verdi.')
                    ctype=response.headers.get('Content-Type','').split(';')[0].lower().strip()
                    if not (ctype.startswith('audio/') or ctype.startswith('video/')):
                        raise HTTPException(422,'Bağlantı doğrudan ses veya video dosyası döndürmüyor.')
                    if int(response.headers.get('Content-Length','0') or 0)>MAX_BYTES:
                        raise HTTPException(413,'Dosya 100 MB sınırını aşıyor.')
                    count=0
                    with src.open('wb') as f:
                        async for chunk in response.content.iter_chunked(65536):
                            count+=len(chunk)
                            if count>MAX_BYTES: raise HTTPException(413,'Dosya 100 MB sınırını aşıyor.')
                            f.write(chunk)
            if not count: raise HTTPException(422,'Dosya boş.')
            args=['ffmpeg','-hide_banner','-loglevel','error','-nostdin','-y','-i',str(src)]
            if req.format=='mp3': args+=['-vn','-codec:a','libmp3lame','-b:a','192k',str(dest)]
            else: args+=['-map','0:v:0','-map','0:a:0?','-c:v','libx264','-preset','veryfast','-crf','24','-c:a','aac','-movflags','+faststart',str(dest)]
            try:
                proc=await asyncio.create_subprocess_exec(*args,stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.PIPE)
                try: _,stderr=await asyncio.wait_for(proc.communicate(),timeout=120)
                except asyncio.TimeoutError:
                    proc.kill();await proc.communicate();raise HTTPException(504,'Dönüştürme zaman aşımına uğradı.')
            except FileNotFoundError: raise HTTPException(503,'FFmpeg sunucuda kurulu değil.')
            if proc.returncode!=0 or not dest.exists(): raise HTTPException(422,'Dosya seçilen formata dönüştürülemedi.')
            if dest.stat().st_size>MAX_BYTES: raise HTTPException(413,'Çıktı 100 MB sınırını aşıyor.')
        return FileResponse(dest,media_type='audio/mpeg' if req.format=='mp3' else 'video/mp4',filename='MonDrop.'+req.format,background=BackgroundTask(temp.cleanup))
    except HTTPException:
        temp.cleanup();raise
    except (aiohttp.ClientError,asyncio.TimeoutError,ValueError,OSError) as exc:
        temp.cleanup();raise HTTPException(422,'Medya dosyası alınamadı veya güvenli bağlantı kurulamadı.') from exc
    except Exception:
        temp.cleanup();raise
