# Weighted Consistent Hashing

这是一个基于原有一致性哈希实现的加权版本，支持为不同的成员设置不同的权重。

## 特性

- **权重支持**: 每个成员可以有不同的权重，权重越高的成员会处理更多的请求
- **负载均衡**: 在权重基础上实现负载均衡，确保高权重成员承担相应比例的负载
- **线程安全**: 所有操作都是线程安全的
- **高性能**: 基于原有优化的一致性哈希实现

## 使用方法

### 1. 实现 WeightedMember 接口

```go
type Server struct {
    name   string
    weight int
}

func (s Server) String() string {
    return s.name
}

func (s Server) Weight() int {
    return s.weight
}
```

### 2. 创建加权一致性哈希

```go
// 创建带权重的成员
members := []consistent.WeightedMember{
    Server{name: "server1", weight: 3}, // 高性能服务器
    Server{name: "server2", weight: 2}, // 中等性能服务器
    Server{name: "server3", weight: 1}, // 低性能服务器
}

// 配置
cfg := consistent.WeightedConfig{
    PartitionCount:    71,
    ReplicationFactor: 10,
    Load:              1.25,
    Hasher:            YourHasher{},
}

// 创建实例
c := consistent.NewWeighted(members, cfg)
```

### 3. 使用 API

```go
// 定位 key
member := c.LocateKey([]byte("my-key"))

// 添加成员
newMember := Server{name: "server4", weight: 4}
c.Add(newMember)

// 移除成员
c.Remove("server1")

// 获取负载分布
loads := c.LoadDistribution()

// 获取权重分布
weights := c.WeightDistribution()

// 获取最近的 N 个成员（用于副本）
replicas, err := c.GetClosestN([]byte("key"), 2)
```

## 权重如何工作

1. **副本数计算**: 每个成员在哈希环上的副本数 = `ReplicationFactor × Weight`
2. **负载分布**: 期望负载 = `平均负载 × 权重`
3. **负载均衡**: 算法确保高权重成员承担相应比例的分区

## 示例

查看 `_examples/weighted_example.go` 文件获取完整的使用示例。

## 性能

在具有 100 个成员的环上，LocateKey 操作的性能约为 **164ns/op**，无内存分配。

## 注意事项

1. **权重范围**: 权重应该是正整数，0 权重会被自动调整为 1
2. **分区数**: 确保分区数足够大以支持所有成员的权重总和
3. **负载因子**: 调整 Load 参数以控制负载均衡的严格程度

## 与原版的区别

| 特性 | 原版 | 加权版 |
|------|------|--------|
| 成员权重 | 所有成员平等 | 支持不同权重 |
| 副本数 | 固定 ReplicationFactor | ReplicationFactor × Weight |
| 负载分布 | 平均分布 | 按权重比例分布 |
| API | Member 接口 | WeightedMember 接口 |
