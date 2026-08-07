# 任务只手动运行且不可恢复

WFT 不提供 Scheduler、自动重试或 stale task 恢复；Ctrl-C 产生 CANCELLED，崩溃残留在下次启动时标记 FAILED，重新执行总是新任务。该决定用简单、可理解的执行语义换取较低自动化，符合一次性安装验证和人工故障诊断场景。
