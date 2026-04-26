<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { 
  Search, 
  Refresh, 
  Download, 
  Star, 
  StarFilled,
  Reading,
  User,
  PriceTag,
  Filter
} from '@element-plus/icons-vue'
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
    const token = localStorage.getItem('token')
    const baseUrl = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:5000'
    const url = `${baseUrl}/api/resources/${item.id}/download?token=${token}`
    window.open(url, '_blank')
  } catch (e) {
    ElMessage.error('下载失败')
  }
}

async function favorite(item) {
  const action = item.is_favorited ? 'unfavorite' : 'favorite'
  try {
    await api.post(`/api/resources/${item.id}/favorite`, { action })
    item.is_favorited = !item.is_favorited
    ElMessage.success(action === 'favorite' ? '已收藏' : '已取消收藏')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  }
}

function openDetail(item) {
  router.push(`/resources/${item.id}`)
}

function resetQuery() {
  query.keyword = ''
  query.tag = ''
  query.course_id = null
  query.knowledge_point_id = null
  query.teacher_id = null
  fetchList()
}

watch(
  () => query.course_id,
  async (v) => {
    query.knowledge_point_id = null
    await fetchKnowledgePoints(v)
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
  <div class="resource-container">
    <!-- Filter Header -->
    <el-card class="filter-card" shadow="never">
      <div class="filter-grid">
        <el-input 
          v-model="query.keyword" 
          placeholder="搜索资源标题/描述" 
          :prefix-icon="Search"
          clearable 
          @change="fetchList"
          class="filter-item"
        />
        
        <el-select v-model="query.course_id" clearable placeholder="所属课程" class="filter-item">
          <template #prefix><el-icon><Reading /></el-icon></template>
          <el-option v-for="c in courses" :key="c.id" :label="c.name" :value="c.id" />
        </el-select>

        <el-select v-model="query.knowledge_point_id" clearable placeholder="关联知识点" class="filter-item" @change="fetchList">
          <el-option v-for="k in knowledgePoints" :key="k.id" :label="k.name" :value="k.id" />
        </el-select>

        <el-select v-model="query.teacher_id" clearable placeholder="授课教师" class="filter-item" @change="fetchList">
          <template #prefix><el-icon><User /></el-icon></template>
          <el-option v-for="t in teachers" :key="t.id" :label="t.name" :value="t.id" />
        </el-select>

        <el-input 
          v-model="query.tag" 
          placeholder="标签筛选" 
          :prefix-icon="PriceTag"
          clearable 
          @change="fetchList"
          class="filter-item"
        />

        <div class="filter-actions">
          <el-button type="primary" :icon="Search" @click="fetchList">搜索</el-button>
          <el-button :icon="Refresh" @click="resetQuery">重置</el-button>
        </div>
      </div>

      <div v-if="isDean" class="status-tabs">
        <el-radio-group v-model="status" @change="fetchList">
          <el-radio-button label="approved">已通过</el-radio-button>
          <el-radio-button label="pending">待审核</el-radio-button>
          <el-radio-button label="rejected">已拒绝</el-radio-button>
        </el-radio-group>
      </div>
    </el-card>

    <!-- Resource Grid -->
    <div v-loading="loading" class="resource-grid">
      <el-empty v-if="items.length === 0" description="暂无符合条件的资源" />
      
      <el-row :gutter="20">
        <el-col v-for="item in items" :key="item.id" :xs="24" :sm="12" :md="8" :lg="6">
          <el-card class="resource-item-card" shadow="hover">
            <div class="resource-type-icon">
              <el-icon :size="40" color="#409eff"><Notebook /></el-icon>
            </div>
            
            <div class="resource-content">
              <h3 class="resource-title" @click="openDetail(item)">{{ item.title }}</h3>
              
              <div class="resource-meta">
                <div class="meta-item">
                  <el-icon><Reading /></el-icon>
                  <span>{{ item.course || '通用课程' }}</span>
                </div>
              </div>

              <div class="resource-tags">
                <el-tag v-for="t in (item.tags || []).slice(0, 3)" :key="t" size="small" effect="plain">
                  {{ t }}
                </el-tag>
              </div>

              <div class="resource-footer">
                <span class="date">{{ new Date(item.created_at).toLocaleDateString() }}</span>
                <div class="actions">
                  <el-tooltip content="收藏" placement="top">
                    <el-button 
                      v-if="canFavorite" 
                      circle 
                      size="small" 
                      :type="item.is_favorited ? 'warning' : 'default'"
                      @click="favorite(item)"
                    >
                      <el-icon><StarFilled v-if="item.is_favorited" /><Star v-else /></el-icon>
                    </el-button>
                  </el-tooltip>
                  <el-tooltip content="下载" placement="top">
                    <el-button circle size="small" :icon="Download" @click="download(item)" />
                  </el-tooltip>
                </div>
              </div>
            </div>
          </el-card>
        </el-col>
      </el-row>
    </div>
  </div>
</template>

<style scoped>
.resource-container {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.filter-card {
  border-radius: 12px;
  background: #fff;
}

.filter-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 16px;
  align-items: center;
}

.filter-actions {
  display: flex;
  gap: 10px;
}

.status-tabs {
  margin-top: 20px;
  border-top: 1px solid #f0f2f5;
  padding-top: 16px;
}

.resource-grid {
  min-height: 400px;
}

.resource-item-card {
  border-radius: 12px;
  margin-bottom: 20px;
  transition: transform 0.3s;
  overflow: hidden;
}

.resource-item-card:hover {
  transform: translateY(-5px);
}

.resource-type-icon {
  background: #f0f7ff;
  height: 100px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: -20px -20px 20px;
}

.resource-content {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.resource-title {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
  color: #303133;
  cursor: pointer;
  transition: color 0.3s;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.resource-title:hover {
  color: var(--primary-color);
}

.resource-meta {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.meta-item {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  color: #909399;
}

.resource-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  height: 24px;
  overflow: hidden;
}

.resource-footer {
  margin-top: 8px;
  padding-top: 12px;
  border-top: 1px solid #f0f2f5;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.resource-footer .date {
  font-size: 12px;
  color: #c0c4cc;
}

.resource-footer .actions {
  display: flex;
  gap: 8px;
}

@media (max-width: 768px) {
  .filter-grid {
    grid-template-columns: 1fr;
  }
}
</style>
