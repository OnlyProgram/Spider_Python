from scrapy import Spider,Request
import re
from scrapy import Selector
from weibospider.items import BaseInfoItem,BaseinfoMap,TweetsInfoItem,TweetsItem,FollowItem,FanItem
from urllib import parse
import time,json
import logging
import redis



class sinaSpider(Spider):
    name = "weibo"
    host = "https://weibo.cn"
    #allowed_domains = ['weibo.cn']
    redis_key = "weiboSpider:start_urls"
    time = time.clock()#计算的秒数返回当前的CPU时间。用来衡量不同程序的耗时
    infocount=0
    tweetscount=0
    requestcount=0
    rconn = redis.Redis()

    def timed_task(self, dalay):
        if time.clock()-self.time >= dalay:
            self.time=time.clock()
            msg='获取用户信息{}条 ，获取微博{}条，向引擎提交请求{}次 '.format(self.infocount,self.tweetscount,self.requestcount)
            logging.info(msg)#报告事件,发生在一个程序的正常运行:logging.info()或logging.debug()

    def start_requests(self): #该方法只执行一次


        list = [5441323079, 5515046766, 3474663810, 2827446132, 2137681634,
               6030812339, 5100872689, 274277335, 1611429274, 2815278241,
               2152931380, 2774781151, 2815822460, 3318070191, 2569940871,
               5581749168, 5587248897, 1934527213]

        url = 'https://weibo.cn/{}/info'
        # 遍历要爬取的用户
        for uid in list:
            url = 'https://weibo.cn/{}/info'
            url = url.format(uid)
            print(url)
            yield Request(url,self.parse_user_info, priority=33,dont_filter=False)

    def parse_user_info(self, response):
        #print("执行psrse_user_info")
        self.timed_task(10)
        print(response.url)
        if response.status==200:  #请求成功
            selector=Selector(response)
            userinfo=BaseInfoItem()
            try:
                temp=re.search(r'(\d+)/info', response.url)  #找出ID
                #print(response.url)
                if temp:
                    ID = temp[1]  #根据链接提取用户ID
                else:
                    return
                infotext=";end".join(selector.xpath('body/div[@class="c"]//text()').extract())  # 获取标签里的所有text并以（；end）隔开
                for key in BaseinfoMap.keys():  #遍历
                    value=BaseinfoMap.get(key)  #根据键名获取键值
                    #print("value为",value)
                    temp=re.search('{}:(.*?);end'.format(value), infotext) #根据名称获取信息
                    if temp:
                        userinfo[key]= temp[1]
                Viplevel=re.search('会员等级.+?(\d{1,2})级\s+;end', infotext)
                if Viplevel:
                    userinfo['Viplevel']=int(Viplevel[1])
                else:
                    userinfo['Viplevel']=0
                userinfo['Id']= ID
                #scrapy会对request的URL去重(RFPDupeFilter)，加上dont_filter则告诉它这个URL不参与去重
                yield Request(url="https://weibo.cn/u/{}?page=1".format(ID), callback=self.parse_tweets,meta={'baseitem':userinfo,'nickname':userinfo['NickName']},dont_filter=True,priority=12)
                yield Request(url="https://weibo.cn/{}/follow".format(ID), callback=self.parse_relationship,meta={'info':'follow','id':ID,'list':[]}, dont_filter=True,priority=9)
                yield Request(url="https://weibo.cn/{}/fans".format(ID), callback=self.parse_relationship,meta={'info':'fans','id':ID,'list':[]}, dont_filter=True,priority=9)
                self.requestcount+=3

            except Exception as e:
                logging.info(e)

    def parse_tweets(self, response):
        self.timed_task(10)
        if response.status==200:
            selector = Selector(response)
            max_crawl_page=50  #设置了最大页数
            try:
                Nickname = response.meta.get('nickname')
                Id=re.search(r'u/(\d+)', response.url)[1]
                current_page = int(re.search(r'page=(\d*)', response.url)[1])
                if current_page == 1:                                        #抽取微博数量信息
                    item = response.meta['baseitem']

                    infotext=''.join(selector.xpath('//div[@class="tip2"]//text()').extract())
                    Tweets = re.search('微博\[(\d+)\]', infotext)[1]  # 微博数
                    Follows = re.search('关注\[(\d+)\]', infotext)[1]  # 关注数
                    Fans = re.search('粉丝\[(\d+)\]', infotext)[1]  # 粉丝数
                    for key in TweetsInfoItem.fields:
                        try:
                            item[key]=eval(key)
                        except NameError:
                            logging.info('Field is Not Defined', key)
                    yield item
                    self.infocount+=1


                divs = selector.xpath('body/div[@class="c" and @id]')
                for weibo in divs:
                    weiboitem=TweetsItem()
                    NickName=Nickname
                    id = Id+'-'+weibo.xpath('@id').extract_first()
                    IsTransfer = bool(weibo.xpath('.//span[@class="cmt"]').extract_first())
                    Content=''.join(weibo.xpath('.//span[@class="ctt"]//text()').extract())
                    Like=int(weibo.xpath('.//a[contains(text(), "赞[")]/text()').re_first('赞\[(.*?)\]'))
                    Transfer = int(weibo.xpath('.//a[contains(text(), "转发[")]/text()').re_first('转发\[(.*?)\]'))
                    # Comment = weibo.xpath('//a[contains(text(), "评论[") and not(contains(text(), "原文"))]//text()').re_first('评论\[(.*?)\]')
                    Comment = int(weibo.xpath('.//a[re:test(text(),"^评论\[")]/text()').re_first('评论\[(.*?)\]'))
                    timeandtools=weibo.xpath('div/span[@class="ct"]/text()')
                    if re.search('来自',''.join(timeandtools.extract())):          #有的微博网页发的 没有来自.....
                        PubTime=timeandtools.re_first('(.*?)\\xa0')
                        Tools=timeandtools.re_first('来自(.*)')
                    else:
                        PubTime=''.join(timeandtools.extract())
                        Tools=''
                    Co_oridinates=weibo.xpath('div/a[re:test(@href,"center=([\d.,]+)")]').re_first("center=([\d.,]+)")
                    for key in weiboitem.fields:
                        if key != 'CommentsList' and key != 'TransferList':
                            try:
                                weiboitem[key] = eval(key)
                            except NameError:
                                logging.info('Field is Not Defined', key)
                    if len(weiboitem['Content'])>=0:
                        commentlist = []
                        transferHref = weibo.xpath('.//a[re:test(text(),"^转发\[")]/@href').extract_first()
                        if weiboitem['Comment']>=1:
                            commentHref=weibo.xpath('.//a[re:test(text(),"^评论\[")]/@href').extract_first()
                            yield Request(url=commentHref, callback=self.parse_comments,
                                          meta={'weiboitem':weiboitem,'comments':commentlist,
                                                'transferHref':transferHref},dont_filter=True,priority=28)
                        else:
                            if weiboitem['Transfer']>=1 and transferHref:
                                yield Request(url=transferHref, callback=self.parse_transfer,
                                              meta={'weiboitem': weiboitem, 'comments': commentlist,
                                                    }, dont_filter=True, priority=28)
                            else:
                                weiboitem['CommentsList'] = []
                                weiboitem['TransferList'] = []
                                yield weiboitem
                                self.tweetscount += 1

                        
                if current_page<max_crawl_page:              #持续获取下一页直到max页面限制
                    next_page=selector.xpath('body/div[@class="pa" and @id="pagelist"]//a[contains(text(),"下页")]/@href').extract_first()
                    if next_page:
                        next_page=parse.urljoin(response.url,next_page)
                        yield Request(next_page, callback=self.parse_tweets,dont_filter=True,priority=13,meta={'nickname':Nickname})
                        self.requestcount+=1
            except Exception as e:
                logging.info(e)

    def parse_comments(self,response):
        self.timed_task(10)
        selector=Selector(response)
        commentlist=response.meta.get('comments')
        commentsdiv=selector.xpath('//div[@class="c" and starts-with(@id,"C")]')
        for comment in commentsdiv:
            try:
                nickname=comment.xpath('./a/text()').extract_first()
                temp=comment.xpath('./a/@href').extract_first()
                uid=str(temp).split('/')[-1]
                content=''.join(comment.xpath('./span[@class="ctt"]//text()').extract())
                like=int(comment.xpath('.//a[contains(text(), "赞[")]/text()').re_first('赞\[(.*?)\]'))
                if content and len(content)>0:
                    commentlist.append({'name':nickname,'uid':uid,'comment':content,'like':like})
            except Exception as e:
                pass
        next_url = selector.xpath('//a[text()="下页"]/@href').extract_first()
        if next_url and len(commentlist)<3000 :
            next_url = parse.urljoin(response.url, next_url)
            response.meta['comments']=commentlist
            yield Request(next_url,priority=28, meta=response.meta,callback=self.parse_comments, dont_filter=True)
        else:
            weiboitem=response.meta.get('weiboitem')
            transferHref = response.meta.get('transferHref')
            if weiboitem['Transfer']>=1 and transferHref :
                yield Request(url=transferHref, callback=self.parse_transfer,
                              meta={'weiboitem': weiboitem, 'comments': commentlist,
                                    }, dont_filter=True, priority=29)
            else:
                weiboitem['TransferList']=[]
                weiboitem['CommentsList']=commentlist
                yield weiboitem
                self.tweetscount += 1


    def parse_transfer(self, response):
        self.timed_task(10)
        transferlist=response.meta.get('transferlist',[])
        selector = Selector(response)
        transferdiv = selector.xpath('//div[@class="c" and not (@id)]')
        for div in transferdiv:
            text=''.join(div.xpath('.//text()').extract())
            if "来自" in text:
                try:
                    nickname = div.xpath('./a/text()').extract_first()
                    temp = div.xpath('./a/@href').extract_first()
                    uid = str(temp).split('/')[-1]
                    like = int(div.xpath('.//a[contains(text(), "赞[")]/text()').re_first('赞\[(.*?)\]'))
                    content=re.search(nickname+'\:(.*)赞\[.*',text).group(1)
                    if content and len(content) > 0:
                        transferlist.append({'name': nickname, 'uid': uid, 'content': content, 'like': like})
                except Exception as e:
                    pass
        next_url = selector.xpath('//a[text()="下页"]/@href').extract_first()
        if next_url and len(transferlist) < 3000:
            next_url = parse.urljoin(response.url, next_url)
            response.meta['transferlist']=transferlist
            yield Request(next_url, priority=30,meta=response.meta,callback=self.parse_transfer, dont_filter=True)
        else:
            weiboitem=response.meta.get('weiboitem')
            weiboitem['TransferList'] = transferlist
            weiboitem['CommentsList'] = response.meta.get('comments')
            yield weiboitem
            self.tweetscount+=1


    def parse_relationship(self, response):
        if response.status==200:
            selector = Selector(response)
            try:
                urls = selector.xpath('//a[text()="关注他" or text()="关注她"]/@href').extract()
                uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
                list=response.meta.get('list')
                rediskey = 'weibo:requests'
                count = self.rconn.zcard(rediskey)
                for uid in uids:
                    list.append(uid)
                    if int(count)<=10000:
                        yield Request(url="https://weibo.cn/{}/info".format(uid), callback=self.parse_user_info)
                        self.requestcount += 1
                next_url = selector.xpath('//a[text()="下页"]/@href').extract_first()
                info = response.meta.get('info')
                id = response.meta.get('id')
                if next_url:
                    next_url=parse.urljoin(response.url,next_url)
                    yield Request(next_url, callback=self.parse_relationship,meta={'info':info,'id':id,'list':list}, dont_filter=True,priority=11)
                    self.requestcount += 1
                else:
                    if info=='follow':relationitem=FollowItem()
                    elif info=='fans':relationitem=FanItem()
                    relationitem['Id']=id
                    relationitem['List']=list
                    yield relationitem

            except Exception as e:
                logging.info(e)



