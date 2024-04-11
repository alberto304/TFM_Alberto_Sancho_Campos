from TikTokApi import TikTokApi
import asyncio
import os
import json
import pandas as pd

ms_token = os.environ.get("a45393400f6870fbf685c14f88efff3627c36989", None)  # token generado desde TikTok ads manager


datos=pd.read_excel("videos_realmadrid1.xlsx", converters={'Cuenta':str, 'URL':str})


lista=[]
for i in range(len(datos)):
    lista.append(str(datos.iloc[i]['URL']))

dicc={}

async def get_video_example():
    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3)
        for i in range(len(lista)):
            video = api.video(url=lista[i])

            async for related_video in video.related_videos(count=10):
                print(related_video)
                print(related_video.as_dict)

            video_info = await video.info()
            print(video_info)
            dicc[str([i+1])]=video_info
            with open( ("dicc_realmadrid2.json"), "w") as j:
               json.dump(dicc, j)



if __name__ == "__main__":
    asyncio.run(get_video_example())

