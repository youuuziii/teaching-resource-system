<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { 
  Refresh, 
  Collection, 
  Clock, 
  Star,
  ArrowRight,
  Monitor,
  Reading,
  Histogram
} from '@element-plus/icons-vue'
import api from '../api/client'

const router = useRouter()
const tab = ref('favorites')

const loadingHistory = ref(false)
const historyItems = ref([])

const loadingFavorites = ref(false)
const favorites = ref([])

const loadingRecommend = ref(false)
const recommendations = ref([])
const detailDialogVisible = ref(false)
const detailCurrent = ref(null)

function formatDateTime(value) {
  if (!value) return '-'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? '-' : d.toLocaleString()
}

function actionLabel(action) {
  const map = {
    view: '浏览',
    favorite: '收藏',
    unfavorite: '取消收藏',
    download: '下载',
  }
  return map[action] || action || '未知'
}

const groupedFavorites = computed(() => {
  const groups = {}
  favorites.value.forEach(item => {
    const courseName = item.course || '通用/其他'
    if (!groups[courseName]) {
      groups[courseName] = []
    }
    groups[courseName].push(item)
  })
  return Object.keys(groups).map(name => ({
    name,
    items: groups[name]
  }))
})

const roles = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isStudent = computed(() => roles.value.includes('student'))

function openDetail(res) {
  if (res?.id) router.push(`/resources/${res.id}`)
}

function openRecommendationDetail(row) {
  detailCurrent.value = row || null
  detailDialogVisible.value = true
}

async function fetchHistory() {
  loadingHistory.value = true
  try {
    const resp = await api.get('/api/me/history', { params: { limit: 50 } })
    historyItems.value = resp.data.items || []
  } catch (e) {
    historyItems.value = []
  } finally {
    loadingHistory.value = false
  }
}

async function fetchFavorites() {
  loadingFavorites.value = true
  try {
    const resp = await api.get('/api/me/favorites')
    favorites.value = resp.data.items || []
  } catch (e) {
    favorites.value = []
  } finally {
    loadingFavorites.value = false
  }
}

async function fetchRecommendations() {
  loadingRecommend.value = true
  try {
    const resp = await api.get('/api/resources/recommend')
    const items = Array.isArray(resp.data.items) ? resp.data.items : []
    recommendations.value = items.map((item) => ({
      ...item,
      reasons: Array.isArray(item.reasons) && item.reasons.length ? item.reasons : ['基于你的学习偏好推荐'],
    }))
  } catch (e) {
    recommendations.value = []
  } finally {
    loadingRecommend.value = false
  }
}

function refreshAll() {
  if (tab.value === 'recommend') fetchRecommendations()
  if (tab.value === 'history') fetchHistory()
  if (tab.value === 'favorites') fetchFavorites()
}

function handleRecommendationsUpdated() {
  if (tab.value === 'recommend') {
    fetchRecommendations()
  }
}

function handleLearningHistoryUpdated() {
  fetchHistory()
  fetchFavorites()
}

onMounted(async () => {
  if (isStudent.value) {
    window.addEventListener('recommendations-updated', handleRecommendationsUpdated)
    window.addEventListener('learning-history-updated', handleLearningHistoryUpdated)
    await Promise.all([fetchHistory(), fetchFavorites(), fetchRecommendations()])
  } else {
    router.replace('/')
  }
})

onBeforeUnmount(() => {
  window.removeEventListener('recommendations-updated', handleRecommendationsUpdated)
  window.removeEventListener('learning-history-updated', handleLearningHistoryUpdated)
})
</script>

<template>
  <div class="page-view">
    <el-card class="main-card learning-page" shadow="hover">
      <template #header>
        <div class="page-toolbar learning-toolbar">
          <div class="page-toolbar__title title-section">
            <el-icon :size="22" class="title-icon"><Monitor /></el-icon>
            <div class="title-block">
              <span class="title">学习与推荐中心</span>
              <span class="title-hint">收藏、推荐与最近学习记录</span>
            </div>
          </div>
          <el-button :icon="Refresh" @click="refreshAll" size="small">刷新当前数据</el-button>
        </div>
      </template>

      <el-tabs v-model="tab" class="learning-tabs">
        <!-- Favorites -->
        <el-tab-pane name="favorites">
          <template #label>
            <div class="tab-label">
              <el-icon><Star /></el-icon>收藏夹
            </div>
          </template>
          
          <div v-loading="loadingFavorites" class="favorites-content">
            <el-empty v-if="groupedFavorites.length === 0" description="暂无收藏资源" />
            <el-collapse v-else :default-active="groupedFavorites.map(g => g.name)">
              <el-collapse-item v-for="group in groupedFavorites" :key="group.name" :name="group.name">
                <template #title>
                  <div class="group-header">
                    <el-icon><Reading /></el-icon>
                    <span class="course-name">{{ group.name }}</span>
                    <el-badge :value="group.items.length" type="info" class="count-badge" />
                  </div>
                </template>
                
                <el-table :data="group.items" stripe style="width: 100%">
                  <el-table-column label="资源标题" min-width="240">
                    <template #default="{ row }">
                      <div class="resource-cell" @click="openDetail(row)">
                        <el-icon class="file-icon"><Collection /></el-icon>
                        <span class="res-title">{{ row.title }}</span>
                      </div>
                    </template>
                  </el-table-column>
                  <el-table-column prop="knowledge_point" label="相关知识点" width="220" />
                  <el-table-column label="操作" width="120" fixed="right" align="center">
                    <template #default="{ row }">
                      <el-button type="primary" link :icon="ArrowRight" @click="openDetail(row)">查看</el-button>
                    </template>
                  </el-table-column>
                </el-table>
              </el-collapse-item>
            </el-collapse>
          </div>
        </el-tab-pane>

        <!-- Recommendations -->
        <el-tab-pane name="recommend">
          <template #label>
            <div class="tab-label">
              <el-icon><Histogram /></el-icon>个性化推荐
            </div>
          </template>
          
          <div v-loading="loadingRecommend" class="recommend-section">
            <el-empty v-if="recommendations.length === 0" description="暂无推荐内容" />

            <div v-else class="recommend-grid">
              <el-card v-for="(row, idx) in recommendations" :key="row.resource?.id || idx" class="recommend-card" shadow="hover" @click="openDetail(row.resource)">
                <div class="recommend-card__head">
                  <div class="recommend-badge">{{ idx + 1 }}</div>
                  <div class="recommend-title-wrap">
                    <div class="recommend-title">{{ row.resource?.title }}</div>
                    <div class="recommend-subtitle">
                      {{ row.resource?.course || '通用课程' }}
                      <span v-if="row.resource?.knowledge_point"> · {{ row.resource.knowledge_point }}</span>
                    </div>
                  </div>
                </div>

                <div class="recommend-tags">
                  <el-tag
                    v-for="reason in row.reasons"
                    :key="reason"
                    size="small"
                    type="success"
                    effect="plain"
                  >
                    {{ reason }}
                  </el-tag>
                </div>

                <div class="recommend-card__foot">
                  <el-button type="primary" link :icon="ArrowRight">去学习</el-button>
                  <el-button link @click.stop="openRecommendationDetail(row)">推荐详情</el-button>
                </div>
              </el-card>
            </div>
          </div>
        </el-tab-pane>

        <!-- History -->
        <el-tab-pane name="history">
          <template #label>
            <div class="tab-label">
              <el-icon><Clock /></el-icon>最近学习
            </div>
          </template>
          
          <el-table :data="historyItems" v-loading="loadingHistory" border stripe style="width: 100%">
            <el-table-column label="资源标题" min-width="300">
              <template #default="{ row }">
                <div class="resource-cell" @click="openDetail(row.resource)">
                  <el-icon class="file-icon"><Collection /></el-icon>
                  <span class="res-title">{{ row.resource?.title }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column prop="updated_at" label="最后查看时间" width="220">
              <template #default="{ row }">
                {{ formatDateTime(row.updated_at) }}
              </template>
            </el-table-column>
            <el-table-column prop="action" label="操作类型" width="120">
              <template #default="{ row }">
                <el-tag size="small" type="info" effect="plain">{{ actionLabel(row.action) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="120" fixed="right" align="center">
              <template #default="{ row }">
                <el-button type="primary" link :icon="ArrowRight" @click="openDetail(row.resource)">继续学习</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>

      <el-dialog v-model="detailDialogVisible" title="推荐详情" width="680px">
        <div v-if="detailCurrent" class="recommend-detail-panel">
          <div class="recommend-detail-title">{{ detailCurrent.resource?.title }}</div>
          <div class="recommend-detail-section">
            <div class="section-label">推荐摘要</div>
            <div class="section-content">{{ detailCurrent.detail?.summary || detailCurrent.reasons?.[0] || '基于学习行为和图谱关系推荐' }}</div>
          </div>
          <div class="recommend-detail-section">
            <div class="section-label">行为依据</div>
            <ul class="detail-list">
              <li v-for="(item, idx) in (detailCurrent.detail?.behavior_notes || [])" :key="idx">{{ item }}</li>
              <li v-if="!(detailCurrent.detail?.behavior_notes || []).length">暂无足够的行为依据，使用默认推荐策略</li>
            </ul>
          </div>
          <div class="recommend-detail-section">
            <div class="section-label">图谱路径</div>
            <div v-if="(detailCurrent.detail?.graph_paths || []).length" class="path-list">
              <div v-for="(pathItem, idx) in detailCurrent.detail.graph_paths" :key="idx" class="path-item">
                <div class="path-reason">{{ pathItem.reason }}</div>
                <div class="path-flow">
                  <span v-for="(node, nIdx) in pathItem.path" :key="nIdx" class="path-node">
                    {{ node.name || '未知节点' }}
                  </span>
                </div>
              </div>
            </div>
            <div v-else class="empty-text">当前推荐未命中明确图谱路径，主要基于知识点相似和学习行为</div>
          </div>
          <div v-if="detailCurrent.detail?.fallback" class="recommend-detail-section">
            <div class="section-label">兜底说明</div>
            <div class="section-content">{{ detailCurrent.detail.fallback }}</div>
          </div>
        </div>
      </el-dialog>
    </el-card>
  </div>
</template>

<style scoped>
.learning-page {
  min-height: 720px;
}

.learning-toolbar {
  align-items: flex-start;
}

.learning-page :deep(.el-tabs__content) {
  margin-top: 8px;
}

.learning-page :deep(.el-table) {
  border-radius: 14px;
  overflow: hidden;
}

.recommend-section {
  min-height: 420px;
}

.recommend-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 16px;
}

.recommend-card {
  cursor: pointer;
  border-radius: 16px;
  border: 1px solid #eef2ff;
  transition: transform 0.25s ease, box-shadow 0.25s ease;
}

.recommend-card:hover {
  transform: translateY(-3px);
  box-shadow: 0 10px 28px rgba(69, 100, 245, 0.12);
}

.recommend-card__head {
  display: flex;
  gap: 12px;
  align-items: flex-start;
}

.recommend-badge {
  width: 28px;
  height: 28px;
  border-radius: 999px;
  background: linear-gradient(135deg, #4564f5, #7c8cff);
  color: #fff;
  font-weight: 700;
  font-size: 13px;
  display: flex;
  align-items: center;
  justify-content: center;
  flex: 0 0 auto;
}

.recommend-title-wrap {
  min-width: 0;
  flex: 1;
}

.recommend-title {
  font-size: 15px;
  font-weight: 700;
  color: #303133;
  line-height: 1.4;
}

.recommend-subtitle {
  margin-top: 4px;
  font-size: 12px;
  color: #909399;
}

.recommend-tags {
  margin-top: 12px;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.recommend-card__foot {
  margin-top: 14px;
  display: flex;
  justify-content: flex-end;
}

@media (max-width: 1024px) {
  .recommend-grid {
    grid-template-columns: 1fr;
  }
}

.learning-page :deep(.el-collapse-item__header) {
  border-radius: 12px;
}

.learning-page :deep(.el-table__cell) {
  padding-top: 14px;
  padding-bottom: 14px;
}

.main-card {
  border-radius: 12px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.title-section {
  display: flex;
  align-items: center;
  gap: 10px;
  color: #303133;
}

.title-section .title {
  font-weight: 600;
  font-size: 18px;
}

.learning-tabs {
  margin-top: 10px;
}

.tab-label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 15px;
}

.group-header {
  display: flex;
  align-items: center;
  gap: 12px;
  width: 100%;
}

.group-header .course-name {
  font-weight: 600;
  font-size: 15px;
  color: #303133;
}

.count-badge {
  margin-left: auto;
  margin-right: 20px;
}

.resource-cell {
  display: flex;
  align-items: center;
  gap: 10px;
  cursor: pointer;
  padding: 4px 0;
}

.resource-cell:hover .res-title {
  color: #409eff;
  text-decoration: underline;
}

.file-icon {
  color: #909399;
}

.res-title {
  color: #606266;
  font-weight: 500;
  transition: color 0.3s;
}

.reason-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.empty-text {
  font-size: 13px;
  color: #c0c4cc;
  font-style: italic;
}

:deep(.el-tabs__item.is-active) {
  font-weight: 600;
}

:deep(.el-table__header) {
  background-color: #f5f7fa;
}
</style>
