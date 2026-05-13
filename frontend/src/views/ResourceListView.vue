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
  Filter,
  Notebook
} from '@element-plus/icons-vue'
import api from '../api/client'

const router = useRouter()

const loading = ref(false)
const items = ref([])
const status = ref('approved')
const pagination = ref({ page: 1, pageSize: 10, total: 0 })

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
        page: pagination.value.page,
        page_size: pagination.value.pageSize,
      },
    })
    items.value = resp.data.items || []
    pagination.value.total = resp.data.total ?? resp.data.items?.length ?? 0
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function handlePageChange(page) {
  pagination.value.page = page
  fetchList()
}

function handleSizeChange(size) {
  pagination.value.pageSize = size
  pagination.value.page = 1
  fetchList()
}

async function download(item) {
  try {
    await api.post('/api/action', { action: 'download', resource_id: item.id })
    const token = localStorage.getItem('token')
    const baseUrl = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:5000'
    const url = `${baseUrl}/api/resources/${item.id}/download?token=${token}`
    window.open(url, '_blank')
    window.dispatchEvent(new Event('recommendations-updated'))
  } catch (e) {
    ElMessage.error('下载失败')
  }
}

async function favorite(item) {
  const nextAction = item.is_favorited ? 'unfavorite' : 'favorite'
  const previous = !!item.is_favorited
  item.is_favorited = !previous
  try {
    const resp = await api.post(`/api/resources/${item.id}/favorite`, { action: nextAction })
    if (typeof resp?.data?.is_favorited === 'boolean') {
      item.is_favorited = resp.data.is_favorited
    }
    window.dispatchEvent(new Event('recommendations-updated'))
    ElMessage.success(nextAction === 'favorite' ? '已收藏' : '已取消收藏')
  } catch (e) {
    item.is_favorited = previous
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  }
}

async function openDetail(item) {
  if (!item?.id) return
  try {
    await api.post('/api/action', { action: 'view', resource_id: item.id })
  } catch (e) {
    // ignore tracking failure to avoid blocking navigation
  }
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

async function recordVisibleItems() {
  if (!isAuthed.value || !isStudent.value || !items.value.length) return
  const seenKey = 'resource-list-seen-ids'
  let seen = []
  try {
    seen = JSON.parse(sessionStorage.getItem(seenKey) || '[]')
  } catch {
    seen = []
  }
  const seenSet = new Set(Array.isArray(seen) ? seen : [])
  const targetIds = items.value.map((item) => item.id).filter((rid) => !seenSet.has(rid))
  if (!targetIds.length) return

  await Promise.allSettled(
    targetIds.map((resource_id) => api.post('/api/action', { action: 'view', resource_id })),
  )
  targetIds.forEach((rid) => seenSet.add(rid))
  try {
    sessionStorage.setItem(seenKey, JSON.stringify(Array.from(seenSet)))
  } catch {
    // ignore storage failure
  }
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
  await recordVisibleItems()
})
</script>

<template>
  <div class="page-view resource-page">
    <el-card class="main-card resource-shell" shadow="hover">
      <template #header>
        <div class="page-toolbar resource-toolbar">
          <div class="page-toolbar__title title-section">
            <el-icon :size="22" class="title-icon"><Notebook /></el-icon>
            <div class="title-block">
              <span class="title">资源中心</span>
              <span class="title-hint">搜索、筛选、收藏与下载学习资源</span>
            </div>
          </div>
          <div class="toolbar-actions">
            <el-button type="primary" :icon="Search" @click="fetchList">搜索</el-button>
            <el-button :icon="Refresh" @click="resetQuery">重置</el-button>
          </div>
        </div>
      </template>

      <div class="filter-card-soft">
        <div class="filter-grid">
          <el-input v-model="query.keyword" placeholder="搜索资源标题/描述" :prefix-icon="Search" clearable @change="fetchList" class="filter-item" />
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
          <el-input v-model="query.tag" placeholder="标签筛选" :prefix-icon="PriceTag" clearable @change="fetchList" class="filter-item" />
        </div>

        <div v-if="isDean" class="status-tabs">
          <el-radio-group v-model="status" @change="fetchList">
            <el-radio-button label="approved">已通过</el-radio-button>
            <el-radio-button label="pending">待审核</el-radio-button>
            <el-radio-button label="rejected">已拒绝</el-radio-button>
          </el-radio-group>
        </div>
      </div>

      <div v-loading="loading" class="resource-grid">
        <el-empty v-if="items.length === 0" description="暂无符合条件的资源" />

        <div class="resource-card-grid">
          <el-tooltip v-for="item in items" :key="item.id" :content="item.file_name || item.title" placement="top" :show-after="350" effect="dark">
            <el-card class="resource-item-card" shadow="hover" @click="openDetail(item)">
              <div class="resource-type-icon">
                <el-icon :size="28" color="#4564f5"><Notebook /></el-icon>
              </div>

              <div class="resource-content">
                <h3 class="resource-title">{{ item.title }}</h3>

                <div class="resource-meta">
                  <div class="meta-item">
                    <el-icon><Reading /></el-icon>
                    <span>{{ item.course || '通用课程' }}</span>
                  </div>
                  <div class="meta-item">
                    <el-icon><User /></el-icon>
                    <span>{{ (item.teachers || []).length ? item.teachers.map(t => t.name).join(' / ') : '上传教师未填写' }}</span>
                  </div>
                  <div class="meta-item knowledge-point-item">
                    <el-icon><Notebook /></el-icon>
                    <span>{{ Array.isArray(item.knowledge_points) && item.knowledge_points.length ? item.knowledge_points.map(k => k.name).join(' / ') : (item.knowledge_point || '所属知识点未填写') }}</span>
                  </div>
                </div>

                <div class="resource-tags">
                  <el-tag v-for="t in (item.tags || []).slice(0, 3)" :key="t" size="small" effect="plain">
                    {{ t }}
                  </el-tag>
                </div>

                <div class="resource-footer">
                  <span class="date">{{ new Date(item.created_at).toLocaleDateString() }}</span>
                  <div class="actions" @click.stop>
                    <el-tooltip content="收藏" placement="top">
                      <el-button v-if="canFavorite" circle size="small" :type="item.is_favorited ? 'warning' : 'default'" @click="favorite(item)">
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
          </el-tooltip>
        </div>
      </div>

      <div class="table-pagination resource-pagination">
        <el-pagination
          v-model:current-page="pagination.page"
          v-model:page-size="pagination.pageSize"
          :total="pagination.total"
          :page-sizes="[10, 20, 50]"
          layout="总数, sizes, prev, pager, next, jumper"
          background
          @current-change="handlePageChange"
          @size-change="handleSizeChange"
        />
      </div>
    </el-card>
  </div>
</template>

<style scoped>
.resource-page {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

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

.resource-card-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 14px;
}

.resource-item-card {
  border-radius: 12px;
  transition: transform 0.3s;
  overflow: hidden;
  cursor: pointer;
  min-height: 220px;
}

.resource-item-card:hover {
  transform: translateY(-3px);
}

.resource-type-icon {
  background: #f0f7ff;
  height: 48px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: -14px -14px 8px;
}

.resource-content {
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.resource-title {
  margin: 0;
  font-size: 13px;
  font-weight: 600;
  color: #303133;
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
  gap: 2px;
}

.meta-item {
  display: flex;
  align-items: center;
  gap: 5px;
  font-size: 10px;
  color: #909399;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.knowledge-point-item span {
  white-space: normal;
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
  overflow: hidden;
}

.resource-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  min-height: 18px;
  max-height: 36px;
  overflow: hidden;
}

.resource-footer {
  margin-top: 0;
  padding-top: 6px;
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

@media (max-width: 1400px) {
  .resource-card-grid {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }
}

@media (max-width: 1100px) {
  .resource-card-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 768px) {
  .filter-grid {
    grid-template-columns: 1fr;
  }

  .resource-card-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .resource-item-card {
    cursor: default;
  }
}

@media (max-width: 520px) {
  .resource-card-grid {
    grid-template-columns: 1fr;
  }
}
</style>
