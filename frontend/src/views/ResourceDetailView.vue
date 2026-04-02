<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '../api/client'

const route = useRoute()
const id = computed(() => Number(route.params.id))

const loading = ref(false)
const resource = ref(null)
const createdByUser = ref(null)
const auditedByUser = ref(null)

const isAuthed = computed(() => (localStorage.getItem('token') || '').length > 0)
const roles = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const canFavorite = computed(() => isAuthed.value && roles.value.includes('student'))

async function fetchDetail() {
  loading.value = true
  try {
    const resp = await api.get(`/api/resources/${id.value}`)
    resource.value = resp.data.resource
    createdByUser.value = resp.data.created_by_user
    auditedByUser.value = resp.data.audited_by_user
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function download() {
  if (!resource.value) return
  const url = `${import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:5000'}/api/resources/${resource.value.id}/download`
  window.open(url, '_blank')
}

async function favorite(action) {
  if (!resource.value) return
  try {
    await api.post(`/api/resources/${resource.value.id}/favorite`, { action })
    ElMessage.success(action === 'favorite' ? '已收藏' : '已取消收藏')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  }
}

onMounted(fetchDetail)
</script>

<template>
  <el-card>
    <template #header>
      <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
        <div style="font-weight: 600">资源详情</div>
        <el-tag v-if="resource" :type="resource.status === 'approved' ? 'success' : resource.status === 'pending' ? 'warning' : 'danger'">
          {{ resource.status }}
        </el-tag>
        <el-button v-if="resource" :disabled="!isAuthed" @click="download">下载</el-button>
        <el-button v-if="resource && canFavorite" type="primary" @click="favorite('favorite')">收藏</el-button>
        <el-button v-if="resource && canFavorite" @click="favorite('unfavorite')">取消收藏</el-button>
      </div>
    </template>

    <el-skeleton :loading="loading" animated>
      <template #default>
        <el-descriptions v-if="resource" :column="1" border>
          <el-descriptions-item label="标题">{{ resource.title }}</el-descriptions-item>
          <el-descriptions-item label="课程">{{ resource.course || '-' }}</el-descriptions-item>
          <el-descriptions-item label="知识点">{{ resource.knowledge_point || '-' }}</el-descriptions-item>
          <el-descriptions-item label="教师">
            <span v-if="(resource.teachers || []).length === 0">-</span>
            <el-tag v-for="t in resource.teachers || []" :key="t.id" style="margin-right: 6px" size="small">
              {{ t.name }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="标签">
            <span v-if="(resource.tags || []).length === 0">-</span>
            <el-tag v-for="t in resource.tags || []" :key="t" style="margin-right: 6px" size="small">{{ t }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="描述">{{ resource.description || '-' }}</el-descriptions-item>
          <el-descriptions-item label="文件名">{{ resource.file_name }}</el-descriptions-item>
          <el-descriptions-item label="大小">{{ resource.file_size || '-' }}</el-descriptions-item>
          <el-descriptions-item label="创建者">{{ createdByUser?.username || resource.created_by || '-' }}</el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ resource.created_at || '-' }}</el-descriptions-item>
          <el-descriptions-item label="审核者">{{ auditedByUser?.username || resource.audited_by || '-' }}</el-descriptions-item>
          <el-descriptions-item label="审核时间">{{ resource.audited_at || '-' }}</el-descriptions-item>
          <el-descriptions-item label="审核备注">{{ resource.audit_comment || '-' }}</el-descriptions-item>
        </el-descriptions>
      </template>
    </el-skeleton>
  </el-card>
</template>
