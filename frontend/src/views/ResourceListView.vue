<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '../api/client'

const router = useRouter()

const loading = ref(false)
const items = ref([])
const status = ref('approved')

const courses = ref([])
const knowledgePoints = ref([])
const teachers = ref([])

const query = reactive({
  keyword: '',
  tag: '',
  course_id: null,
  knowledge_point_id: null,
  teacher_id: null,
})

const isAuthed = computed(() => (localStorage.getItem('token') || '').length > 0)
const roles = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isDean = computed(() => roles.value.includes('dean'))
const isStudent = computed(() => roles.value.includes('student'))
const canFavorite = computed(() => isAuthed.value && isStudent.value)

function isNumberValue(v) {
  return typeof v === 'number' && Number.isFinite(v)
}

async function fetchCourses() {
  try {
    const resp = await api.get('/api/courses')
    courses.value = resp.data.items || []
  } catch (e) {
    courses.value = []
  }
}

async function fetchKnowledgePoints(courseId) {
  if (!isNumberValue(courseId)) {
    knowledgePoints.value = []
    return
  }
  try {
    const resp = await api.get('/api/knowledge-points', { params: { course_id: courseId } })
    knowledgePoints.value = resp.data.items || []
  } catch (e) {
    knowledgePoints.value = []
  }
}

async function fetchTeachers() {
  try {
    const resp = await api.get('/api/teachers')
    teachers.value = resp.data.items || []
  } catch (e) {
    teachers.value = []
  }
}

async function fetchList() {
  loading.value = true
  try {
    const resp = await api.get('/api/resources', {
      params: {
        status: isDean.value ? status.value : 'approved',
        keyword: query.keyword || undefined,
        tag: query.tag || undefined,
        course_id: isNumberValue(query.course_id) ? query.course_id : undefined,
        knowledge_point_id: isNumberValue(query.knowledge_point_id) ? query.knowledge_point_id : undefined,
        teacher_id: isNumberValue(query.teacher_id) ? query.teacher_id : undefined,
      },
    })
    items.value = resp.data.items || []
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

async function download(item) {
  try {
    const url = `${import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:5000'}/api/resources/${item.id}/download`
    window.open(url, '_blank')
  } catch (e) {
    ElMessage.error('下载失败')
  }
}

async function favorite(item, action) {
  try {
    await api.post(`/api/resources/${item.id}/favorite`, { action })
    ElMessage.success(action === 'favorite' ? '已收藏' : '已取消收藏')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  }
}

function openDetail(row) {
  router.push(`/resources/${row.id}`)
}

watch(
  () => query.course_id,
  async (v) => {
    query.knowledge_point_id = null
    await fetchKnowledgePoints(v)
    await fetchList()
  },
)

watch(
  () => isDean.value,
  async (v) => {
    if (!v) status.value = 'approved'
    await fetchList()
  },
)

onMounted(async () => {
  await fetchCourses()
  await fetchTeachers()
  await fetchKnowledgePoints(query.course_id)
  await fetchList()
})
</script>

<template>
  <el-card>
    <template #header>
      <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
        <div style="font-weight: 600">资源列表</div>
        <el-select v-model="status" style="width: 140px" :disabled="!isDean" @change="fetchList">
          <el-option label="已通过" value="approved" />
          <el-option v-if="isDean" label="待审核" value="pending" />
          <el-option v-if="isDean" label="已拒绝" value="rejected" />
        </el-select>
        <el-input v-model="query.keyword" placeholder="关键词" style="width: 180px" clearable @change="fetchList" />
        <el-input v-model="query.tag" placeholder="标签" style="width: 160px" clearable @change="fetchList" />
        <el-select v-model="query.course_id" clearable placeholder="课程" style="width: 160px">
          <el-option v-for="c in courses" :key="c.id" :label="c.name" :value="c.id" />
        </el-select>
        <el-select v-model="query.knowledge_point_id" clearable placeholder="知识点" style="width: 180px" @change="fetchList">
          <el-option v-for="k in knowledgePoints" :key="k.id" :label="k.name" :value="k.id" />
        </el-select>
        <el-select v-model="query.teacher_id" clearable placeholder="教师" style="width: 160px" @change="fetchList">
          <el-option v-for="t in teachers" :key="t.id" :label="t.name" :value="t.id" />
        </el-select>
        <el-button :loading="loading" @click="fetchList">刷新</el-button>
      </div>
    </template>

    <el-table :data="items" v-loading="loading" style="width: 100%">
      <el-table-column label="标题" min-width="220">
        <template #default="{ row }">
          <el-link type="primary" @click="openDetail(row)">{{ row.title }}</el-link>
        </template>
      </el-table-column>
      <el-table-column prop="course" label="课程" min-width="120">
        <template #default="{ row }">
          {{ row.course }}
        </template>
      </el-table-column>
      <el-table-column prop="knowledge_point" label="知识点" min-width="140">
        <template #default="{ row }">
          {{ row.knowledge_point }}
        </template>
      </el-table-column>
      <el-table-column label="教师" min-width="140">
        <template #default="{ row }">
          <span v-if="(row.teachers || []).length === 0">-</span>
          <el-tag v-for="t in row.teachers || []" :key="t.id" style="margin-right: 6px" size="small">
            {{ t.name }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="status" label="状态" width="110" />
      <el-table-column label="标签" min-width="160">
        <template #default="{ row }">
          <el-tag v-for="t in row.tags || []" :key="t" style="margin-right: 6px" size="small">{{ t }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="200">
        <template #default="{ row }">
          <el-button size="small" @click="download(row)">下载</el-button>
          <el-button v-if="canFavorite" size="small" type="primary" @click="favorite(row, 'favorite')">收藏</el-button>
          <el-button v-if="canFavorite" size="small" @click="favorite(row, 'unfavorite')">取消</el-button>
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>
