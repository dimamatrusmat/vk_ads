import time

import requests

def getToken():
    payload = {
        'client_id': 'zawG5V96vFJJ1EJI',
        'client_secret': '7E1GWhpLPaCcHtmtayt5C7aPJWSVLx0S2X3C3SgA8BalOuYCZj9JhhE3bL6mEfuVovpj4JHK6OU74JGjk9RKsPjfhqzbuDzm1BgWMJ9kocc9gjPxA6MDChgVY4WFwfuhAtixAIqy4ZWwaRklGQJUbiGIP2w2f2syjea6Ru2gyV75LJR8kBf0riLaycaXM846s401zZhqWXJc831IYdR7AMSLzqvetRxEDMn0Du3PXo2ygUI1vAROOKu',
        'grant_type': 'client_credentials',
        'scope': ["read_ads"] # read_clients
    }
    headers = {'Content-type': 'application/x-www-form-urlencoded'}

    res = requests.post('https://ads.vk.com/api/v2/oauth2/token.json', data=payload, headers=headers)

    return res.text

def deleteToken():
    payload = {
        'client_id': 'zawG5V96vFJJ1EJI',
        'client_secret': '7E1GWhpLPaCcHtmtayt5C7aPJWSVLx0S2X3C3SgA8BalOuYCZj9JhhE3bL6mEfuVovpj4JHK6OU74JGjk9RKsPjfhqzbuDzm1BgWMJ9kocc9gjPxA6MDChgVY4WFwfuhAtixAIqy4ZWwaRklGQJUbiGIP2w2f2syjea6Ru2gyV75LJR8kBf0riLaycaXM846s401zZhqWXJc831IYdR7AMSLzqvetRxEDMn0Du3PXo2ygUI1vAROOKu',
        # 'user_id': "" # read_clients
    }
    headers = {'Content-type': 'application/x-www-form-urlencoded'}

    res = requests.post('https://ads.vk.com/api/v2/oauth2/token/delete.json', data=payload, headers=headers)

    return res.text
token = '6IC4oD8VOouwLP77bA8CV45ZS7gEMp2aZzkSlIp7JPvCGpYZV0h6w5jaBMspCt5mN0dXm8trAVcMZBLhrgAqOEmTILdB0YIsidRsXmrLdvBjZbDecHatLiLMXxhyEfyJseXf3vHgf25bE6H6ejl0rFGdNC9x4IBHdSObP6escKx1hqwEsUQZV4SgtaijBw2OmqvWLWtxTBCZJWF43xl0MJWSe5GchBNGtjwEhNrz4'
refresh_token = 'YAzwIMJnmGl72mXVux7UMG4Sh0PxXQo9HuTKXOyy2ypmXH2I4JLrFUenqwxoG9ZGT0zFXeo64TwLpZv4pOz1dQT5uYzplxG1MzZW66nCYXtGp9Wr30A6eJ7WGoHJDmV330kOv9ipZ7DpWjQuGFZcUPINm7qKnhhaVl6E26iBwhkhdn2uEReo3MVleL58GmXaC0kfzFMBQE13v1zT5fHXcSI3MxB'


# def requests_func(self, method, url_params):
#     payload = {
#         'token':
#     }
#     url = f'https://api.vk.com/method/{method}?v=5.131&access_token={self.TOKEN}&{url_params}*v={self.v}'
#     headers = {'Content-type': 'application/x-www-form-urlencoded'}

#     res = requests.post('https://ads.vk.com/api/v2/oauth2/token/delete.json', data=payload, headers=headers)
"""
{"access_token":"JsZxkJxYbLppUDkHKocgFqYyrdfEOVkEltyqvrN5r7nMTv6D0ptpj7Fbg89jMuyosiv4Em0yS4vXu7gnBqUeYLbIB65jLYUqoxarR1TNSsEargn6fRmD54yidhVxyL0PLvvEjGLpFT0q3e7wFupdWixCKaobWnZRHuejoIKd0E6Z7RpqaIk1w6WfrTOxCvMnFVnesS0jvRjLnQaqzuqnUdHmNkmlpjsRxUTn9zLIlKD","token_type":"Bearer","expires_in":86400,"scope":["read_ads"],"refresh_token":"diqTACluOrZ2q6hSlPj3w6t8XvnI9pcLfI1bAuxtX785xiaLNOqAa0yvKJLUHaFJYStJtlUK35xwgGFx1DelssWai7CuRa7Jc6BPOb7CppiLCX9QkPMvO19FinWxFByHrggH2Wlp66EOHqdQGmCHzZDCXmqs94AbsZ4qZP8gpa2cDXXAOmlmOccsftPp0zJNQzGGAd063YGhTPic1OPKhBQtbbkx7vzj","tokens_left":4}
"""
def getResponse():
    campaign_ids = []
    ads_ids = []
    r = requests.get('https://api.vk.com/method/ads.getAds', params={
        'access_token': token,
        'v': 5.81,
        'account_id': "17288947",
        # 'client_id': "17288947",
    })
    print(r.text)
    data = r.json()['response']

def useToken():
    access_token = 'JsZxkJxYbLppUDkHKocgFqYyrdfEOVkEltyqvrN5r7nMTv6D0ptpj7Fbg89jMuyosiv4Em0yS4vXu7gnBqUeYLbIB65jLYUqoxarR1TNSsEargn6fRmD54yidhVxyL0PLvvEjGLpFT0q3e7wFupdWixCKaobWnZRHuejoIKd0E6Z7RpqaIk1w6WfrTOxCvMnFVnesS0jvRjLnQaqzuqnUdHmNkmlpjsRxUTn9zLIlKD'
    payload = {
        'v': 5.81,
        # 'id': '17288947',
    }
    headers = {'Content-type': 'application/x-www-form-encoded', "Authorization": f"Bearer {access_token}"}

    res = requests.get('https://ads.vk.com/api/v1/urls', params=payload, headers=headers)

    return res.text


def useMethod(method, param):
    access_token = 'JsZxkJxYbLppUDkHKocgFqYyrdfEOVkEltyqvrN5r7nMTv6D0ptpj7Fbg89jMuyosiv4Em0yS4vXu7gnBqUeYLbIB65jLYUqoxarR1TNSsEargn6fRmD54yidhVxyL0PLvvEjGLpFT0q3e7wFupdWixCKaobWnZRHuejoIKd0E6Z7RpqaIk1w6WfrTOxCvMnFVnesS0jvRjLnQaqzuqnUdHmNkmlpjsRxUTn9zLIlKD'

    headers = {'Content-type': 'application/x-www-form-encoded', "Authorization": f"Bearer {access_token}"}
    url = f'https://ads.vk.com{method}'


    res = requests.get(url, params=param, headers=headers)

    return res.text
def getStatistick():
    access_token = 'JsZxkJxYbLppUDkHKocgFqYyrdfEOVkEltyqvrN5r7nMTv6D0ptpj7Fbg89jMuyosiv4Em0yS4vXu7gnBqUeYLbIB65jLYUqoxarR1TNSsEargn6fRmD54yidhVxyL0PLvvEjGLpFT0q3e7wFupdWixCKaobWnZRHuejoIKd0E6Z7RpqaIk1w6WfrTOxCvMnFVnesS0jvRjLnQaqzuqnUdHmNkmlpjsRxUTn9zLIlKD'
    payload = {
        'v': 5.81,
        # 'id': '17288947',
        'date_from': '2023-07-04',
        'date_to': '2023-07-20'
    }
    headers = {'Content-type': 'application/x-www-form-encoded', "Authorization": f"Bearer {access_token}"}

    res = requests.get('https://ads.vk.com/api/v2/statistics/users/day.json', params=payload, headers=headers)

    return res.text

def main():
    payload = {
        'v': 5.81,
        # 'id': '17288947',
        # 'date_from': '2024-01-01',
        # 'date_to': '2024-01-05'
    }
    d = useMethod('/api/v2/remarketing/segments.json', payload)

    print(d)
if __name__ == '__main__':
    dd = deleteToken()
    # dd = getToken()
    # dd = getStatistick()
    print(dd)
    # main()