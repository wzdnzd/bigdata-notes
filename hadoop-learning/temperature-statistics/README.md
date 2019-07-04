# Temperature Statistics

## 需求
> 数据里包含`xxxx`年`xx`月`xx`日的气温数据，同一天可能存在多个时间点的气温数据，统计出每个月气温最高的两天的气温

## 实现
> 通过`hadoop`的`MapReduce`框架实现


## 思路
> 先找出每一天的最高气温，通过`setPartitionerClass`函数设置分区类使得相同年份的数据进入到同一个`reduce`里，`setSortComparatorClass`自定义排序，`setGroupingComparatorClass`相同年份和月份的数据进入同一个`values`执行同一个`reduce`,最后利用`数组里找出最大值和次大值`算法便能实现需求

## 给定一个整形数组，找出最大值和次大值
```java
public static int[] findMax(int[] nums) {
    if (nums == null || nums.length <= 1)
        return nums;

    int[] result = {Integer.MIN_VALUE, Integer.MIN_VALUE};

    for (int num : nums) {
        if (num > result[0]) {
            result[1] = result[0];
            result[0] = num;
        } else if (num > result[1] && num < result[0])
            result[1] = num;
    }
    return result;
}
```
