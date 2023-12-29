Xunsearch迅搜客户端Java版
---

目前仅完成search部分方法，仅能用于部分只读场景。

已完成可用方法：
- Search类
    - setScwsMulti
    - setQuery
    - setFacets
    - addRange
    - addQueryString
    - setMultiSort
    - setLimit
    - search
    - getLastCount
    - setGeodistSort
    - setFuzzy
    - setCutoff
    - addWeight
    - getFacets
    - count
- Index类（未开发）



大致使用方式
```
try(XsSearch xs = (new Xs(project)).getSearch()) {
  xs.setQuery("新城")
    .setFacets(new String[]{"status"}, true)
    .addRange("status", 1, 1)
    .addRange("price", 8000,12000 )
    .addRange("is_new", 1, 1)
    .setMultiSort(new LinkedHashMap<>(){{
        put("sort", false);
        put("created", false);
    }})
    .setLimit(10, 0);
    List<XsDocument> docs = xs.search();
}
```