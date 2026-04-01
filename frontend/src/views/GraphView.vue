<script setup>
import { onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import * as echarts from 'echarts'
import api from '../api/client'

const loading = ref(false)
const chartEl = ref(null)
let chart = null

const course = ref('')
const graph = ref({ nodes: [], links: [], source: '' })

async function load() {
  loading.value = true
  try {
    const resp = await api.get('/api/graph/overview', { params: { course: course.value || undefined } })
    graph.value = resp.data
    render()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function render() {
  if (!chartEl.value) return
  if (!chart) chart = echarts.init(chartEl.value)

  const nodes = (graph.value.nodes || []).map((n) => ({
    id: n.id,
    name: n.label || n.id,
    value: n.type,
    category: n.type,
    symbolSize: n.type === 'resource' ? 18 : n.type === 'knowledge_point' ? 26 : 30,
  }))
  const links = (graph.value.links || []).map((e) => ({
    source: e.source,
    target: e.target,
    value: e.type,
  }))
  const categories = Array.from(new Set(nodes.map((n) => n.category))).map((c) => ({ name: c }))

  chart.setOption({
    tooltip: {},
    legend: [{ data: categories.map((c) => c.name) }],
    series: [
      {
        type: 'graph',
        layout: 'force',
        roam: true,
        draggable: true,
        data: nodes,
        links,
        categories,
        force: { repulsion: 120, edgeLength: 90 },
        label: { show: true },
      },
    ],
  })
}

onMounted(() => {
  load()
  window.addEventListener('resize', () => chart?.resize())
})

watch(course, () => load())
</script>

<template>
  <el-card>
    <template #header>
      <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
        <div style="font-weight: 600">知识图谱</div>
        <el-input v-model="course" placeholder="课程过滤（可选）" style="width: 220px" clearable />
        <el-tag type="info">{{ graph.source || 'unknown' }}</el-tag>
        <el-button :loading="loading" @click="load">刷新</el-button>
      </div>
    </template>
    <div ref="chartEl" style="height: 560px; width: 100%" />
  </el-card>
</template>
