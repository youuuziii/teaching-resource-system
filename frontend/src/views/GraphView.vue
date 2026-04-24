<script setup>
import { onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import * as echarts from 'echarts'
import api from '../api/client'

const router = useRouter()

const loading = ref(false)
const chartEl = ref(null)
let chart = null

const course = ref('')
const isFull = ref(false)
const graph = ref({ nodes: [], links: [], source: '' })

const drawerOpen = ref(false)
const selectedNode = ref(null)
const resourceLoading = ref(false)
const resourceItems = ref([])

async function load() {
  loading.value = true
  try {
    const resp = await api.get('/api/graph/overview', {
      params: { 
        course: course.value || undefined, 
        level: isFull.value ? 'full' : 'courses' 
      },
    })
    graph.value = resp.data
    render()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function mergeGraph(partial) {
  const nextNodes = partial?.nodes || []
  const nextLinks = partial?.links || []

  const nodeMap = new Map((graph.value.nodes || []).map((n) => [n.id, n]))
  for (const n of nextNodes) {
    if (!n?.id) continue
    const prev = nodeMap.get(n.id)
    if (!prev) nodeMap.set(n.id, n)
    else nodeMap.set(n.id, { ...prev, ...n, label: n.label || prev.label })
  }

  const linkKey = (e) => `${e.source}__${e.target}__${e.type}`
  const linkMap = new Map((graph.value.links || []).map((e) => [linkKey(e), e]))
  for (const e of nextLinks) {
    if (!e?.source || !e?.target) continue
    const k = linkKey(e)
    if (!linkMap.has(k)) linkMap.set(k, e)
  }

  graph.value = {
    ...(graph.value || {}),
    nodes: Array.from(nodeMap.values()),
    links: Array.from(linkMap.values()),
  }
}

async function exploreByNode(node) {
  if (!node?.id) return
  resourceLoading.value = true
  try {
    const nodeType = node?.value || node?.category
    const isCourse = nodeType === 'course'
    const isKnowledgePoint = nodeType === 'knowledge_point'
    const resp = await api.get('/api/graph/explore', {
      params: {
        node_id: node.id,
        depth: isKnowledgePoint ? 2 : 1,
        expand: isCourse ? 'kps' : isKnowledgePoint ? 'resources' : 'full',
      },
    })
    mergeGraph(resp.data)
    render()
    
    selectedNode.value = node
    
    if (isKnowledgePoint) {
      resourceItems.value = resp.data.items || []
      drawerOpen.value = true
    } else {
      drawerOpen.value = false
      resourceItems.value = []
      
      // 如果点击的是资源节点，直接跳转详情
      if (nodeType === 'resource') {
        const rid = node.id.split(':')[1]
        if (rid) router.push(`/resources/${rid}`)
      }
    }
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    resourceLoading.value = false
  }
}

function openDetail(row) {
  router.push(`/resources/${row.id}`)
}

function render() {
  if (!chartEl.value) return
  if (!chart) chart = echarts.init(chartEl.value)

  const nodes = (graph.value.nodes || []).map((n) => ({
    id: n.id,
    name: n.label || n.id,
    value: n.type,
    category: n.type,
    symbolSize: n.type === 'resource' ? 18 : (n.type === 'knowledge_point' || n.type === 'department') ? 26 : 30,
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

  chart.off('click')
  chart.on('click', (params) => {
    if (params?.dataType !== 'node') return
    exploreByNode(params.data)
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
        <el-input v-model="course" placeholder="课程过滤（可选）" style="width: 200px" clearable />
        <el-switch
          v-model="isFull"
          active-text="展示全部"
          inactive-text="基础视图"
          @change="load"
        />
        <el-tag type="info">{{ graph.source || 'unknown' }}</el-tag>
        <el-button :loading="loading" @click="load">刷新</el-button>
      </div>
    </template>
    <div ref="chartEl" style="height: 560px; width: 100%" />
  </el-card>

  <el-drawer v-model="drawerOpen" :with-header="true" size="520px">
    <template #header>
      <div style="display: flex; align-items: center; gap: 8px">
        <div style="font-weight: 600">关联资源</div>
        <el-tag type="info">{{ selectedNode?.name || selectedNode?.id }}</el-tag>
      </div>
    </template>
    <el-table :data="resourceItems" v-loading="resourceLoading" style="width: 100%">
      <el-table-column label="标题" min-width="220">
        <template #default="{ row }">
          <el-link type="primary" @click="openDetail(row)">{{ row.title }}</el-link>
        </template>
      </el-table-column>
      <el-table-column prop="course" label="课程" min-width="120" />
      <el-table-column prop="knowledge_point" label="知识点" min-width="140" />
      <el-table-column label="教师" min-width="140">
        <template #default="{ row }">
          <span v-if="(row.teachers || []).length === 0">-</span>
          <el-tag v-for="t in row.teachers || []" :key="t.id" style="margin-right: 6px" size="small">{{ t.name }}</el-tag>
        </template>
      </el-table-column>
    </el-table>
  </el-drawer>
</template>
