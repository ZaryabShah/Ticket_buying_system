import requests

url = "https://ticket.yes24.com/Pages/English/Perf/FnPerfList.aspx#"

querystring = {"Genre":"15456"}

payload = ""
headers = {
    "cookie": "ASP.NET_SessionId=aagtlftbspvboyqd1fxl3xxq",
    "^Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7^",
    "^Accept-Language": "en-US,en;q=0.9^",
    "^Cache-Control": "max-age=0^",
    "^Connection": "keep-alive^",
    "^Referer": "https://ticket.yes24.com/Pages/English/Member/FnSignUp.aspx^",
    "^sec-ch-ua": "^\^Not",
    "^Sec-Fetch-Dest": "document^",
    "^Sec-Fetch-Mode": "navigate^",
    "^Sec-Fetch-Site": "same-origin^",
    "^Sec-Fetch-User": "?1^",
    "^Upgrade-Insecure-Requests": "1^",
    "^User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36^",
    "Cookie": "^ASP.NET_SessionId=ryfptsmgpwslfdrs0t2bluau; _fwb=46qeCofiPrwBBKSNFY2rhH.1751562567988; __utmc=186092716; __utmz=186092716.1751562568.1.1.utmcsr=(direct)^|utmccn=(direct)^|utmcmd=(none); BM-session-id=mcnn4r10-4x25gsf3cc2ngzdjg; BM-session-id=mcnn6gay-4vavo6jnk1pioaz7x; _gcl_au=1.1.25886804.1751562653; _ga=GA1.1.763627785.1751562654; PCID=17515626542216122976810; cartCookieCnt=0; TS012911cf=0188f690ad234db0b2b4e344a90d7a921a16779e5375fb7a9185388ef1406fa13435055cbfe8c7af73fa31f9e2613de1ec52f7262c; viewedItems=148370146^%^2C148369548; _ga_8VDEZE64EB=GS2.1.s1751571684^^^^^^; __utma=186092716.1113926393.1751562568.1751571336.1751577261.4; __utmt=1; BM-login-id=; wcs_bt=s_1b6883469aa6:1751577413; __utmb=186092716.8.10.1751577261; spf_sid_key=VFZSak1VMVVWVE5PZWxFeVRWRTlQUT09; spf_sid_key=VFZSak1VMVVWVE5PZWxFeFQxRTlQUT09^"
}

response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
with open("response.html", "w", encoding="utf-8") as file:
    file.write(response.text)
# print(response.text)