

开务数据库

# 功能规格书









*<span style="color: rgb(143,149,158); background-color: inherit">&lt;&lt;功能名称&gt;&gt;</span>*

*<span style="color: rgb(143,149,158); background-color: inherit">&lt;&lt;成员&gt;&gt;</span>*





**&#x20;**

# 1. **文档说明**

## 1.1 术语与缩写解释

| <span style="color: inherit; background-color: rgb(242,243,245)">缩写、术语</span> | <span style="color: inherit; background-color: rgb(242,243,245)">解 释</span> |
| ----------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| 节点                                                                            | KaiwuDB节点,是运行运行数据库服务的物理服务器,多个节点构成一个集群.                                      |
| 集群                                                                            | 多个KaiwuDB数据库服务拓扑形态，至少由3个节点构成.                                               |
|                                                                               |                                                                             |
|                                                                               |                                                                             |



# 2. **规格介绍**

## 2.1 **摘要**

### 2.1.1 **对需求的简要说明**

***&#x20;**<span style="color: rgb(36,91,219); background-color: inherit">&lt; 需求是什么，i.e.说明用户需要解决的问题是什么 &gt;</span>*

| **需求池ID**                                                               | **Devops需求ID**                                                          | **需求标题**                                                                | **其它**                                                                  |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| <span style="color: rgb(36,91,219); background-color: inherit"> </span> | <span style="color: rgb(36,91,219); background-color: inherit"> </span> | <span style="color: rgb(36,91,219); background-color: inherit"> </span> | <span style="color: rgb(36,91,219); background-color: inherit"> </span> |
| <span style="color: rgb(36,91,219); background-color: inherit"> </span> | <span style="color: rgb(36,91,219); background-color: inherit"> </span> | <span style="color: rgb(36,91,219); background-color: inherit"> </span> | <span style="color: rgb(36,91,219); background-color: inherit"> </span> |
| <span style="color: rgb(36,91,219); background-color: inherit"> </span> | <span style="color: rgb(36,91,219); background-color: inherit"> </span> | <span style="color: rgb(36,91,219); background-color: inherit"> </span> | <span style="color: rgb(36,91,219); background-color: inherit"> </span> |

### 2.1.2 **对解决方案的简要说明**

&#x20;*<span style="color: rgb(36,91,219); background-color: inherit">&lt;少量几句话给出解决方案概要介绍,无需细节&gt;</span>*



&#x20;

## 2.2 **面向客户群体和场景**

*<span style="color: rgb(36,91,219); background-color: inherit">&lt;介绍功能面向的客户群体,比如IOT时序数据,在线交易,在线分析,等等;并介绍功能描述中会覆盖到的用户角色都有哪些,比如数据库管理员,数据库应用开发人员等&gt;</span>*

***&#x20;***

## 2.3 需要满足的**国家或行业标准**

*<span style="color: rgb(36,91,219); background-color: inherit">&lt;本特性应当遵循的国际/国家或者行业标准或规范&gt;</span>*

&#x20;

## 2.4 **详细的问题声明和解决方案大纲**

*<span style="color: rgb(36,91,219); background-color: inherit">&lt;本条目要解决的所有问题说明,视情况增减问题列表&gt;</span>*

### 2.4.1 **问题1**

#### 2.4.1.1 声明和影响

*<span style="color: rgb(36,91,219); background-color: inherit">--- 问题1的故事性说明，描述用户在什么样的情况下需要做什么操作，会遇到什么样的问题</span>*



#### 2.4.1.2 解决方案说明

*<span style="color: rgb(36,91,219); background-color: inherit">--- 问题1的解决方案</span>*



# 3. **功能规格详述以及对现有产品的影响**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 本章需详尽列出对产品的各个方面和组件所做的改变,给出改变的细节以及对现有产品的影响</span>*

## 3.1 底层依赖

### 3.1.1 硬件依赖

*<span style="color: rgb(36,91,219); background-color: inherit">--- 本解决方案是否支持特定的硬件平台，例如特定的CPU种类, 特定网卡类型, 特定类型/指标的硬盘</span>*



### 3.1.2 操作系统依赖和限制

*<span style="color: rgb(36,91,219); background-color: inherit">--- 是否依赖特定操作系统种类, 版本, 或发行版,   </span>*

*<span style="color: rgb(36,91,219); background-color: inherit">-- 是否依赖操作系统中特定设置或开关</span>*



### 3.1.3 系统库/第三方库依赖

*<span style="color: rgb(36,91,219); background-color: inherit">--- 是否依赖用户环境安装特定系统软件, 系统库, 或者特定第三方软件, 第三方库?  </span>*

*<span style="color: rgb(36,91,219); background-color: inherit">--  或本次特定开发是否改变了原有系统库/第三方库的依赖?</span>*



## 3.2 **SQL语言更改**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 如果本解决方案有需要修改SQL语言，详细描述该部分的规格以及变化对现有产品的影响,如果本解决方案有需要修改SQL语言;否则填写不适用</span>*



## 3.3 **命令行更改（kwbase或任何二进制工具）**

*<span style="color: rgb(36,91,219); background-color: inherit">-- 例如kwbase启动命令参数是否有变化，kwbase sql/import 等子命令是否有参数变化  </span>*

*<span style="color: rgb(36,91,219); background-color: inherit">-- 包括kwbase启动参数的变化</span>*



## 3.4 配置**参数更改**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 详细描述该部分的规格以及变化对现有产品的影响，如果本解决方案有需要修改数据库参数；否则填写不适用</span>*

### 3.4.1 cluster setting

*<span style="color: rgb(36,91,219); background-color: inherit">--增加, 减少, 变更?</span>*



### 3.4.2 环境变量

*<span style="color: rgb(36,91,219); background-color: inherit">--增加, 减少, 变更?</span>*



## 3.5 安装部署和默认配置

*<span style="color: rgb(36,91,219); background-color: inherit">--- 本特性是否需要安装部署时进行额外处理</span>*

### 3.5.1 运行时第三方依赖变更

*<span style="color: rgb(36,91,219); background-color: inherit">--- 如果本解决方案需要改变客户安装运行环境中的第三方依赖包，或者需要在安装部署时对改变安装行为、改变特定配置，请在这里说明。 例如需要在客户安装前预先安装某系统库，需要调整某个系统配置等。</span>*

*<span style="color: rgb(36,91,219); background-color: inherit">//  操作系统的参数配置?</span>*

*<span style="color: rgb(36,91,219); background-color: inherit">//  安装时需要考虑第三方依赖包的处理?</span>*



### 3.5.2 安装和数据**目录结构更改**

*<span style="color: rgb(36,91,219); background-color: inherit">//  安装介质是否有文件变化,  安装部署目录/文件变化,  或数据文件的位置变化</span>*



### 3.5.3 安装部署的默认配置变化

*<span style="color: rgb(36,91,219); background-color: inherit">//  安装部署中是否需要做额外配置</span>*



## 3.6 **机制变化**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 详细描述该部分的规格以及变化对现有产品的影响,如果本解决方案有需要修改行为数据,比如增删改新的或现有的机制;否则填写不适用</span>*





## 3.7 版本兼容性和升级&#x20;

### 3.7.1 **是否改变系统表，如何实现版本升级和兼容**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述是否会改变系统表，如果适用；否则填写不适用</span>*

&#x20;

&#x20;***&#x20;***

### 3.7.2 **是否改变基础数据，如何实现版本升级和兼容**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述是否会改变基础数据，如果适用；否则填写不适用</span>*

&#x20;

&#x20;

### 3.7.3 **是否改变跨网络调用的数据包(包括RPC消息体)，如何实现版本升级和兼容**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述是否会改变跨网络调用数据包，如果适用；否则填写不适用</span>*

&#x20;

***&#x20;***

### 3.7.4 **是否改变数据、WAL日志的存储格式，如何实现版本升级和兼容**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述是否会改变数据、WAL日志的存储格式，如果适用；否则填写不适用</span>*

&#x20;

&#x20;

### 3.7.5 **是否改变系统变量、系统/内置函数，如何实现版本升级和兼容**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述是否会改变系统变量、系统函数，如果适用；否则填写不适用</span>*



### 3.7.6  **用户自定义对象的变化，如何实现版本升级和兼容**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 详细描述该部分的规格以及变化对现有产品的影响，如果本解决方案有需要修改用户自定义的存储过程/函数/触发器/数据类型；否则填写不适用</span>*



## 3.8 JDBC/ODBC或其它**接口变更**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 如果本解决方案需要修改数据库同其它环境或系统交互的软件设备接口,比如改变statement cache的行为或信息，详细描述该部分的规格以及变化对现有产品的影响,否则填写不适用</span>*



## 3.9 报错**消息新增和更改**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 如果本解决方案有需要修改报错消息，详细涉及的错误码（error code）和错误信息（error messasge）</span>*



## 3.10 **特殊的用户表和文件**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 如果本解决方案有需要修改用户定义的对象或者文件，详细描述该部分的规格以及变化对现有产品的影响，否则填写不适用</span>*



## 3.11 对其它现有功能的**依赖和影响**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 详细描述本解决方案对所依赖的或者依赖于本数据库的其它产品或者解决方案可能带来的影响和改变，例如要增加过期数据清理（数据生命周期）功能会和已有的压缩功能有重叠；如没有，填写不适用</span>*



## 3.12 **版本归属（开源版 vs 企业版）**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 哪些功能属于开源版， 哪些功能属于企业版？</span>*



## 3.13 **其他变更**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 详细描述该部分的规格以及变化对现有产品的影响，如果以上诸点没有涵盖到；否则填写不适用</span>*



# 4. **功能局限以及其他注意事项**

## 4.1 参照产品PRD要求的功能局限

*<span style="color: rgb(36,91,219); background-color: inherit">1）这个特性实现相对于产品提出的PRD的功能范围是否存在限制？ 例如产品要求支持同步、异步2种模式，这次只实现了一种模式；</span>*



## 4.2 参照业界平均标准/典型实现的功能局限

*<span style="color: rgb(36,91,219); background-color: inherit">2）本次开发特性完成后，我们的产品在这个功能所在的方面，相对于市场上其它竞品、用户典型场景是否存在明显限制。 例如对于&quot;支持隔离级别&quot;这个需求, 业界典型实现为4种隔离级别, 如果仅实现2种, 不支持的局限要在这里描述清楚</span>*



## 4.3 当前设计实现内在局限

*<span style="color: rgb(36,91,219); background-color: inherit">3）本次开发交付的功能点存在哪些限制 ？ 例如本功能在某些配置、条件下不能使用；某些配置、条件下产品有特殊行为，等等。</span>*



# 5. **场景演示**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 使用一到多个用户案例描述本解决方案是如何工作的，2.4节描述场景的具象化描述，视情况增减用户场景数量</span>*

## 5.1 场景用例1

### 5.1.1 **方案说明**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述用户用例1</span>*



### 5.1.2 **用户行为描述**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述该场景下用户需要采取的操作或者观察到的变化</span>*



### 5.1.3 **系统行为描述**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述该场景下系统所做的相应反应或操作及带来的相应变化</span>*



## 5.2 **场景用例2**

### 5.2.1 **方案说明**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述用户用例2</span>*

### 5.2.2 **用户行为描述**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述该场景下用户需要采取的操作或者观察到的变化</span>*

### 5.2.3 **系统行为描述**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述该场景下系统所做的相应反应或操作及带来的相应变化</span>*



# 6. **非功能变化事项**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述该解决方案对于非功能性方面的评估</span>*

## 6.1 **性能**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 如果本解决方案提升或者降低数据库的性能，详细描述该部分的规格以及变化对现有产品的影响</span>*

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述新引入的功能的性能目标，如果适用；否则填写不适用</span>*



## 6.2 信息安全和**保护**

### 6.2.1 **对新功能的安全要求**

**&#x20;    &#x20;*****<span style="color: rgb(36,91,219); background-color: inherit">  </span>**<span style="color: rgb(36,91,219); background-color: inherit">--- 描述新引入的功能的安全方面的需求，如果适用；否则填写不适用</span>*

**&#x20;**

### 6.2.2 **权限控制**

**&#x20;  &#x20;*****<span style="color: rgb(36,91,219); background-color: inherit">    </span>**<span style="color: rgb(36,91,219); background-color: inherit">--- 描述新引入的功能的权限控制方面的要求，如果适用；否则填写不适用</span>*



### 6.2.3 **license控制**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 描述新引入的功能的license控制方面的要求，如果适用；否则填写不适用</span>*

&#x20;

### 6.2.4 **数据加密**

*<span style="color: rgb(36,91,219); background-color: inherit">-- 描述新引入的功能的数据加密方面的要求，如果适用；否则填写不适用</span>*



### 6.2.5 **审计要求**

&#x20;      *<span style="color: rgb(36,91,219); background-color: inherit"> --- 描述新引入的功能的审计方面的需求，如果适用；否则填写不适用</span>*

&#x20;



## 6.3 **可用性和**稳定**性**

### 6.3.1 **新功能是否/如何支持高可用性**

*<span style="color: rgb(36,91,219); background-color: inherit">--</span> <span style="color: rgb(36,91,219); background-color: inherit">-</span> <span style="color: rgb(36,91,219); background-color: inherit">新增功能是否具备高可用性，在高可用场景下（节点失效、节点恢复、扩缩容等）行为如何，例如是否报错，是否需要用户干预</span>*



### 6.3.2 **对新功能是否降低或限制高可用性**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 新增功能是否造成高可用在某些场景下受限，例如执行某个数据重构操作时不能做及replica迁移</span>*



### 6.3.3 **对系统稳定性的影响**

*<span style="color: rgb(36,91,219); background-color: inherit">&lt;描述新功能或者新支持的场景对整个系统稳定性的影响。清楚的描述这个特性对系统的哪些部件有稳定性的影响。&gt;</span>*



### 6.3.4 系统资源要求和使用情况

*<span style="color: rgb(36,91,219); background-color: inherit">&lt; 针对占用大量CPU、内存等系统资源的特性，不涉及写 N/A &gt;</span>*

#### 6.3.4.1 内存使用变化分析和说明



#### 6.3.4.2 存储空间变化分析和说明



#### 6.3.4.3 CPU使用变化分析和说明



#### 6.3.4.4 网络和其它资源使用变化分析和说明



### 6.3.5 高并发情况下的行为说明

*<span style="color: rgb(36,91,219); background-color: inherit">&lt; 描述高并发情况下，不同连接之间的互斥同步行为 &gt;</span>*



### 6.3.6 容错性和疲劳分析

*<span style="color: rgb(36,91,219); background-color: inherit">&lt; 描述如果遇到用户误操作造成大量错误，包括预期错误和非预期错误时，系统可能会有哪些表现或变化 &gt;</span>*



## 6.4 监控和运维

*<span style="color: rgb(36,91,219); background-color: inherit">&lt;描述此功能可能涉及的运维方面的特性，不涉及写 N/A &gt;</span>*



### 6.4.1 此功能关联的可以用来监控的命令、工具、接口、监控指标包括哪些

*<span style="color: rgb(36,91,219); background-color: inherit">&lt; 例如show jobs可以显示当前运行的后台任务， 再例如explain可以获取执行计划&gt;</span>*



### 6.4.2 此功能常见问题如何诊断和分析，请结合新增的命令、日志项等进行说明

*<span style="color: rgb(36,91,219); background-color: inherit">&lt; 遇到错误如何诊断。 尽量靠出错信息帮用户明确和定位问题，但是对于一些较复杂但很可能遇到的问题，需要有诊断和分析手段&gt;</span>*



### 6.4.3 是否新增系统任务等需要日常运维关注的内容





# 7. **产品文档影响**

| 文档名称                | 文档目的                                      | 内容变化类型<br />（新增/更新/删减） | 内容变化摘要 |
| ------------------- | ----------------------------------------- | ---------------------- | ------ |
| KaiwuDB安装部署手册       | 介绍如何安装和部署KaiwuDB, 包括安装前准备、执行和验证。          |                        |        |
| KaiwuDB  SQL参考手册    | 介绍KaiwuDB 支持的数据类型、函数、操作符以及SQL语句。          |  <br />                |        |
| KaiwuDB 运维手册        | 介绍如何配置、管理、监控和优化数据库；如何进行问题排查、修复和报告。        |                        |        |
| KaiwuDB 编程指南        | 介绍如何开发针对KaiwuDB 的应用程序                     |                        |        |
| KaiwuDB 数据导入导出手册    | 介绍如何使用SQL语句导出导入KaiwuDB 的数据。               |                        |        |
| KaiwuDB  数据库迁移手册    | 介绍如何使用KaiwuDB数据库迁移工具进行单表、多表、单库及多库迁移。      |                        |        |
| KaiwuDB  开发者中心用户手册  | 介绍如何安装和使用KaiwuDB开发者中心客户端（KDC）。            |                        |        |
| KaiwuDB  预测分析引擎用户手册 | 介绍如何安装和使用KaiwuDB预测分析引擎进行机器学习相关的模型管理和预测分析。 |                        |        |
| KaiwuDB 错误码参考手册     | 介绍KaiwuDB 的错误码信息、错误原因和操作措施。               |                        |        |
|                     |                                           |                        |        |





# 8. **设计变更请求（DCR）**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 本章节列举针对需求需要做的设计变更,总体设计结束后如有需要进行小的设计改变,则需要提出设计变更需求,设计改变需求视情况可有多次.</span>*



## 8.1 **DCR1**

### 8.1.1 **摘要**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 概要描述本设计需求所要解决的问题和本需求提出的解决方案。</span>*



### 8.1.2 **DCR描述**

*<span style="color: rgb(36,91,219); background-color: inherit">--- 详细描述本需求提出的解决方案。</span>*



### 8.1.3 **DCR审批人**



### 8.1.4 **DCR完成日期**

