import csv
with open ('source.txt', 'r', encoding='utf-8') as f:
    contents = f.read()
contents = contents.split('\n\n')
numb_entries = (len(contents))
numb_entries_new = int(contents[0].split('\n')[0])
# print(numb_entries_new)
# print(numb_entries)
# print((contents[0]))
# print(numb_entries)
# print(contents[629814])

def contents_tsv(contents):
    papers = open('papers.tsv', 'w', encoding='UTF8', newline='')
    year = open('year.tsv', 'w', encoding='UTF8', newline='')
    venue = open('venue.tsv', 'w', encoding='UTF8', newline='')
    mainauthor = open('mainauthor.tsv', 'w', encoding='UTF8', newline='')
    coauthor = open('coauthor.tsv', 'w', encoding='UTF8', newline='')
    referenceslist = open('referenceslist.tsv', 'w', encoding='UTF8', newline='')
    hpapers = ['id', 'title', 'abstract']
    hyear = ['id', 'year']
    hvenue = ['id', 'venue']
    hmainauthor = ['id','mainauthor']
    hcoauthor = ['id','coauthor','orderlevel']
    hreferenceslist = ['id','referenceid']
    wpapers = csv.writer(papers, delimiter='\t')
    wyear = csv.writer(year, delimiter='\t')
    wvenue = csv.writer(venue, delimiter='\t')
    wmainauthor = csv.writer(mainauthor, delimiter='\t')
    wcoauthor = csv.writer(coauthor, delimiter='\t')
    wreferenceslist = csv.writer(referenceslist, delimiter='\t')
    wpapers.writerow(hpapers)
    wyear.writerow(hyear)
    wvenue.writerow(hvenue)
    wmainauthor.writerow(hmainauthor)
    wcoauthor.writerow(hcoauthor)
    wreferenceslist.writerow(hreferenceslist)
    dpapers = {}
    dyear = {}
    dvenue = {}
    dmainauthor = {}
    dcoauthor = {}
    dreferenceslist = {}
    for x in hpapers:
        y = dpapers.setdefault(x)
    for x in hyear:
        y = dyear.setdefault(x)
    for x in dvenue:
        y = dvenue.setdefault(x)
    for x in hmainauthor:
        y = dmainauthor.setdefault(x)
    for x in hcoauthor:
        y = dcoauthor.setdefault(x)
    for x in hreferenceslist:
        y = dreferenceslist.setdefault(x)

    for x in contents:
        if x!=contents[numb_entries_new]:
            tpapers = dpapers
            tyear = dyear
            tvenue = dvenue
            tmainauthor = dmainauthor
            tcoauthor = dcoauthor
            treferenceslist = dreferenceslist
            list_coauthors = []
            x_lines = x.split('\n')
            index_crecord = 0
            for y in x_lines:
                if(y.startswith('#*')):
                    if(y[2:]):
                        s=y[2:]
                        s=s.replace("'", '"')
                        s=s.replace("\\","\\\\")
                        tpapers['title'] = s
                elif(y.startswith('#index')):
                    tpapers['id'] = int(y[6:])
                    index_crecord = int(y[6:])
                    # print(index_crecord)
                elif(y.startswith('#!')):
                    if(y[2:]):
                        s=y[2:]
                        s=s.replace("'", '"')
                        s=s.replace("\\","\\\\")
                        tpapers['abstract'] = s
                    else:
                        tpapers['abstract'] = "This is a paper of ID "+str(tpapers['id'])+" with the main concept on "+tpapers['title']
                elif(y.startswith('#t')):
                    if(y[2:]):
                        tyear['year'] = int(y[2:])
                elif(y.startswith('#c')):
                    if(y[2:]):
                        tvenue['venue'] = (y[2:])
                elif(y.startswith('#%')):
                    if(y[2:]):
                        treferenceslist['id'] = index_crecord
                        treferenceslist['referenceid'] = int(y[2:])
                        if(treferenceslist['referenceid']!=tpapers['id']):
                            res = list(map(treferenceslist.get, hreferenceslist))
                            wreferenceslist.writerow(res)
                            treferenceslist = dreferenceslist
                elif(y.startswith('#@')):
                    if(y[2:]):
                        list_authors = y[2:].split(',')
                        tmainauthor['mainauthor'] = list_authors[0]
                        for la in range(1,len(list_authors)):
                            if(list_authors[la]):
                                list_coauthors.append(list_authors[la])
            count = 0
            for tt in list_coauthors:
                temp =  {}
                temp['id'] = index_crecord
                temp['coauthor'] = tt
                count+=1
                temp['orderlevel'] = count
                wcoauthor.writerow(list(map(temp.get,hcoauthor)))
            tyear['id'] = index_crecord
            tvenue['id'] = index_crecord
            tmainauthor['id'] = index_crecord
            # print(list(map(tpapers.get, hpapers)))
            if(tpapers['abstract']==None):
                tpapers['abstract'] = "This is a paper of ID "+str(tpapers['id'])+" with the main concept on "+tpapers['title']
            wpapers.writerow(list(map(tpapers.get, hpapers)))
            wmainauthor.writerow(list(map(tmainauthor.get, hmainauthor)))
            wyear.writerow(list(map(tyear.get, hyear)))
            wvenue.writerow(list(map(tvenue.get, hvenue)))
            tyear['year'] = None
            tvenue['venue'] = None
            tpapers['abstract']=None
            tcoauthor['coauthor']=None
            tmainauthor['mainauthor']=None
            treferenceslist['referenceid'] = None
contents_tsv(contents)
