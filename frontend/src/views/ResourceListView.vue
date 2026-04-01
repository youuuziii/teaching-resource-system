<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api/client'

const loading = ref(false)
const items = ref([])
const status = ref('approved')

const query = reactive({
  keyword: '',
  tag: '',
})

const isAuthed = computed(() => (localStorage.getItem('token') || '').length > 0)

const upload = reactive({
  title: '',
  description: '',
  course: '',
  knowledge_point: '',
  tags: '',
  file: null,
})

async function fetchList() {
  loading.value = true
  try {
    const resp = await api.get('/api/resources', {
      params: {
        status: status.value,
        keyword: query.keyword || undefined,
        tag: query.tag || undefined,
      },
    })
    items.value = resp.data.items || []
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function onFileChange(ev) {
  const f = ev?.target?.files?.[0]
  upload.file = f || null
}

async function submitUpload() {
  if (!upload.file) {
    ElMessage.warning('请选择文件')
    return
  }
  const fd = new FormData()
  fd.append('file', upload.file)
  fd.append('title', upload.title || upload.file.name)
  if (upload.description) fd.append('description', upload.description)
  if (upload.course) fd.append('course', upload.course)
  if (upload.knowledge_point) fd.append('knowledge_point', upload.knowledge_point)
  if (upload.tags) fd.append('tags', upload.tags)

  try {
    await api.post('/api/resources/upload', fd, { headers: { 'Content-Type': 'multipart/form-data' } })
    ElMessage.success('上传成功，等待审核')
    upload.title = ''
    upload.description = ''
    upload.course = ''
    upload.knowledge_point = ''
    upload.tags = ''
    upload.file = null
    await fetchList()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '上传失败')
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

onMounted(fetchList)
</script>

<template>
  <el-row :gutter="16">
    <el-col :xs="24" :md="16">
      <el-card>
        <template #header>
          <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
            <div style="font-weight: 600">资源列表</div>
            <el-select v-model="status" style="width: 140px" @change="fetchList">
              <el-option label="已通过" value="approved" />
              <el-option label="待审核" value="pending" />
              <el-option label="已拒绝" value="rejected" />
            </el-select>
            <el-input v-model="query.keyword" placeholder="关键词" style="width: 180px" clearable @change="fetchList" />
            <el-input v-model="query.tag" placeholder="标签" style="width: 160px" clearable @change="fetchList" />
            <el-button type="primary" :loading="loading" @click="fetchList">刷新</el-button>
          </div>
        </template>

        <el-table :data="items" v-loading="loading" style="width: 100%">
          <el-table-column prop="title" label="标题" min-width="220" />
          <el-table-column prop="course" label="课程" min-width="120" />
          <el-table-column prop="knowledge_point" label="知识点" min-width="140" />
          <el-table-column prop="status" label="状态" width="110" />
          <el-table-column label="标签" min-width="160">
            <template #default="{ row }">
              <el-tag v-for="t in row.tags || []" :key="t" style="margin-right: 6px" size="small">{{ t }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="200">
            <template #default="{ row }">
              <el-button size="small" @click="download(row)">下载</el-button>
              <el-button size="small" type="primary" @click="favorite(row, 'favorite')">收藏</el-button>
              <el-button size="small" @click="favorite(row, 'unfavorite')">取消</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-card>
    </el-col>

    <el-col :xs="24" :md="8">
      <el-card>
        <template #header>上传资源</template>
        <el-alert v-if="!isAuthed" type="warning" show-icon :closable="false" style="margin-bottom: 12px">
          登录后才能上传、收藏与下载未审核资源
        </el-alert>

        <el-form label-width="80px" @submit.prevent="submitUpload">
          <el-form-item label="标题">
            <el-input v-model="upload.title" :disabled="!isAuthed" />
          </el-form-item>
          <el-form-item label="描述">
            <el-input v-model="upload.description" :disabled="!isAuthed" type="textarea" :rows="3" />
          </el-form-item>
          <el-form-item label="课程">
            <el-input v-model="upload.course" :disabled="!isAuthed" />
          </el-form-item>
          <el-form-item label="知识点">
            <el-input v-model="upload.knowledge_point" :disabled="!isAuthed" />
          </el-form-item>
          <el-form-item label="标签">
            <el-input v-model="upload.tags" :disabled="!isAuthed" placeholder="逗号分隔" />
          </el-form-item>
          <el-form-item label="文件">
            <input type="file" accept=".pdf,.doc,.docx" :disabled="!isAuthed" @change="onFileChange" />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" :disabled="!isAuthed" @click="submitUpload">上传</el-button>
          </el-form-item>
        </el-form>
      </el-card>
    </el-col>
  </el-row>
</template>
