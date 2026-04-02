<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '../api/client'

const router = useRouter()

const loading = ref(false)
const user = ref(null)
const tab = ref('recommend')

const editOpen = ref(false)
const editForm = ref({ phone: '', password: '' })

const loadingHistory = ref(false)
const historyItems = ref([])

const loadingFavorites = ref(false)
const favorites = ref([])

const loadingRecommend = ref(false)
const recommendations = ref([])

const isAuthed = computed(() => (localStorage.getItem('token') || '').length > 0)
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
  router.push(`/resources/${res.id}`)
}

async function fetchMe() {
  loading.value = true
  try {
    const resp = await api.get('/api/me')
    user.value = resp.data.user
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function openEdit() {
  editForm.value = { phone: user.value?.phone || '', password: '' }
  editOpen.value = true
}

async function submitEdit() {
  const payload = {}
  const phone = (editForm.value.phone || '').trim()
  if (phone) payload.phone = phone
  const password = (editForm.value.password || '').trim()
  if (password) payload.password = password

  if (Object.keys(payload).length === 0) {
    editOpen.value = false
    return
  }

  try {
    await api.put('/api/me', payload)
    ElMessage.success('保存成功')
    editOpen.value = false
    await fetchMe()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '保存失败')
  }
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

onMounted(async () => {
  if (!isAuthed.value) return
  await fetchMe()
  if (isStudent.value) {
    await Promise.all([fetchHistory(), fetchFavorites(), fetchRecommendations()])
  }
})
</script>

<template>
  <el-row :gutter="16">
    <el-col :xs="24" :md="10">
      <el-card>
        <template #header>
          <div style="display: flex; justify-content: space-between; align-items: center">
            <span>个人信息</span>
            <el-button v-if="isAuthed" size="small" @click="openEdit">编辑</el-button>
          </div>
        </template>
        <el-alert v-if="!isAuthed" type="warning" show-icon :closable="false">
          请先登录后查看个人中心
        </el-alert>
        <el-skeleton v-else :loading="loading" animated>
          <template #default>
            <el-descriptions v-if="user" :column="1" border>
              <el-descriptions-item label="用户 ID">{{ user.id }}</el-descriptions-item>
              <el-descriptions-item label="用户名">{{ user.username }}</el-descriptions-item>
              <el-descriptions-item label="角色">{{ (user.roles || []).join(', ') }}</el-descriptions-item>
              <el-descriptions-item label="手机号">{{ user.phone || '-' }}</el-descriptions-item>
            </el-descriptions>
          </template>
        </el-skeleton>
      </el-card>
    </el-col>

    <el-col :xs="24" :md="14">
      <el-card>
        <template #header>
          <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
            <div style="font-weight: 600">学习与推荐</div>
            <el-button v-if="isAuthed && isStudent" @click="fetchHistory">刷新轨迹</el-button>
            <el-button v-if="isAuthed && isStudent" @click="fetchFavorites">刷新收藏</el-button>
            <el-button v-if="isAuthed && isStudent" type="primary" @click="fetchRecommendations">刷新推荐</el-button>
          </div>
        </template>

        <el-alert v-if="isAuthed && !isStudent" type="info" show-icon :closable="false">
          当前角色无学习轨迹、收藏夹与推荐功能
        </el-alert>

        <el-tabs v-else v-model="tab">
          <el-tab-pane label="推荐" name="recommend">
            <el-table :data="recommendations" v-loading="loadingRecommend" style="width: 100%">
              <el-table-column label="资源" min-width="220">
                <template #default="{ row }">
                  <el-link type="primary" @click="openDetail(row.resource)">{{ row.resource?.title }}</el-link>
                </template>
              </el-table-column>
              <el-table-column label="理由" min-width="260">
                <template #default="{ row }">
                  <el-text>{{ (row.reasons || []).join('；') || '-' }}</el-text>
                </template>
              </el-table-column>
            </el-table>
          </el-tab-pane>

          <el-tab-pane label="学习轨迹" name="history">
            <el-table :data="historyItems" v-loading="loadingHistory" style="width: 100%">
              <el-table-column label="资源" min-width="220">
                <template #default="{ row }">
                  <el-link type="primary" @click="openDetail(row.resource)">{{ row.resource?.title }}</el-link>
                </template>
              </el-table-column>
              <el-table-column prop="viewed_at" label="时间" min-width="200" />
            </el-table>
          </el-tab-pane>

          <el-tab-pane label="收藏夹" name="favorites">
            <el-table :data="favorites" v-loading="loadingFavorites" style="width: 100%">
              <el-table-column label="资源" min-width="220">
                <template #default="{ row }">
                  <el-link type="primary" @click="openDetail(row)">{{ row.title }}</el-link>
                </template>
              </el-table-column>
              <el-table-column prop="course" label="课程" min-width="120" />
              <el-table-column prop="knowledge_point" label="知识点" min-width="140" />
            </el-table>
          </el-tab-pane>
        </el-tabs>
      </el-card>
    </el-col>
  </el-row>

  <el-dialog v-model="editOpen" title="编辑个人信息" width="420px">
    <el-form :model="editForm" label-width="80px">
      <el-form-item label="手机号">
        <el-input v-model="editForm.phone" placeholder="请输入手机号" />
      </el-form-item>
      <el-form-item label="新密码">
        <el-input v-model="editForm.password" placeholder="留空则不修改" show-password />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="editOpen = false">取消</el-button>
      <el-button type="primary" @click="submitEdit">保存</el-button>
    </template>
  </el-dialog>
</template>
