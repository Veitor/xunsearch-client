Xunsearch迅搜客户端Java版
---


## 使用方式

搜索：
```
try(XsSearch xs = (new Xs(project)).getSearch()) {
  xs.setQuery("关键词")
    .setFacets(new String[]{"status"}, true)
    .addRange("status", 1, 1)
    .addRange("price", 8000,12000 )
    .setMultiSort(new LinkedHashMap<>(){{
        put("sort", false);
        put("created", false);
    }})
    .setLimit(10, 0);
    List<XsDocument> docs = xs.search();
}
```

管理索引：
```
try(XsIndex xs = (new Xs(project)).getIndex()) {
  XsDocument doc = new XsDocument();
}
```


## 可能存在的问题

1. 目前是假设所有搜索数据的字符都是包含在Unicode字符集的BMP中，因此部分字符串处理过程中是按照code unit进行处理的，而不是按照PHP版本中按字节处理，若出现可能疑似字符处理相关的问题可提issue。
2. 可能还有部分常用函数未完成，但绝大多数常用的函数多可以使用。

> 目前还处于内部使用阶段，暂不建议在生产环境中使用