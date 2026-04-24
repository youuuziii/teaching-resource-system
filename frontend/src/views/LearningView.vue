<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { 
  Refresh, 
  Collection, 
  Clock, 
  Star,
  ArrowRight,
  Monitor,
  Reading
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
    const resp = await api.get('/api/recommendations')
    recommendations.value = resp.data.items || []
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

onMounted(async () => {
  if (isStudent.value) {
    await Promise.all([fetchHistory(), fetchFavorites(), fetchRecommendations()])
  } else {
    router.replace('/')
  }
})
</script>

<template>
  <div class="learning-container">
    <el-card class="main-card" shadow="never">
      <template #header>
        <div class="card-header">
          <div class="title-section">
            <el-icon :size="20"><Monitor /></el-icon>
            <span class="title">学习与推荐中心</span>
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
              <el-icon><Star /></el-icon>智能推荐
            </div>
          </template>
          
          <el-table :data="recommendations" v-loading="loadingRecommend" border stripe style="width: 100%">
            <el-table-column label="推荐资源" min-width="240">
              <template #default="{ row }">
                <div class="resource-cell" @click="openDetail(row.resource)">
                  <el-icon class="file-icon"><Collection /></el-icon>
                  <span class="res-title">{{ row.resource?.title }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="推荐理由" min-width="300">
              <template #default="{ row }">
                <div class="reason-tags">
                  <el-tag v-for="reason in (row.reasons || [])" :key="reason" size="small" type="success" effect="plain">
                    {{ reason }}
                  </el-tag>
                  <span v-if="!row.reasons?.length" class="empty-text">系统基于您的学习偏好推荐</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="120" fixed="right" align="center">
              <template #default="{ row }">
                <el-button type="primary" link :icon="ArrowRight" @click="openDetail(row.resource)">去学习</el-button>
              </template>
            </el-table-column>
          </el-table>
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
            <el-table-column prop="viewed_at" label="最后查看时间" width="220">
              <template #default="{ row }">
                {{ new Date(row.viewed_at).toLocaleString() }}
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
    </el-card>
  </div>
</template>

<style scoped>
.learning-container {
  padding: 0;
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
